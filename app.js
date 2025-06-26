const express = require('express');
const { createClient } = require('@supabase/supabase-js');
require('dotenv').config();

const app = express();
const PORT = process.env.PORT || 8080;

// Middleware
app.use(express.json());
app.use(express.static('public'));

// Supabase client
const supabase = createClient(
  process.env.SUPABASE_URL,
  process.env.SUPABASE_ANON_KEY
);

// Environment validation
const requiredEnvVars = ['SUPABASE_URL', 'SUPABASE_ANON_KEY'];
const missingEnvVars = requiredEnvVars.filter(varName => !process.env[varName]);

if (missingEnvVars.length > 0) {
  console.error('❌ Missing required environment variables:', missingEnvVars);
  process.exit(1);
}

// Health check endpoint
app.get('/', (req, res) => {
  res.json({
    status: 'healthy',
    service: 'SMG Cloud Automation Pipeline',
    phase: '2',
    modules: [
      '/smg-daily-dates ✅',
      '/smg-transform ✅', 
      '/smg-upload ✅',
      '/smg-pipeline 🔄',
      '/smg-status 🔄'
    ],
    timestamp: new Date().toISOString()
  });
});

// MODULE 1: SMG Daily Dates - Simple 3 calendar day calculation
app.get('/smg-daily-dates', async (req, res) => {
  try {
    console.log('📅 Starting SMG daily dates calculation...');
    
    const today = new Date();
    const dates = [];
    
    // Calculate the last 3 calendar days (including today)
    for (let i = 0; i < 3; i++) {
      const date = new Date(today);
      date.setDate(today.getDate() - i);
      dates.push(date.toISOString().split('T')[0]); // Format as YYYY-MM-DD
    }
    
    const result = {
      success: true,
      dates: dates,
      count: dates.length,
      calculation_method: '3_calendar_days',
      reference_date: today.toISOString().split('T')[0],
      timestamp: new Date().toISOString()
    };
    
    console.log('✅ SMG daily dates calculated:', result);
    res.json(result);
    
  } catch (error) {
    console.error('❌ SMG daily dates error:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// MODULE 2: SMG Transform - CSV transformation using multi-year-transformer logic
app.post('/smg-transform', async (req, res) => {
  try {
    console.log('🔄 Starting SMG CSV transformation...');
    
    const { csvData, date } = req.body;
    
    if (!csvData) {
      return res.status(400).json({
        error: 'Missing required parameter: csvData',
        timestamp: new Date().toISOString()
      });
    }
    
    if (!date) {
      return res.status(400).json({
        error: 'Missing required parameter: date',
        timestamp: new Date().toISOString()
      });
    }
    
    // Parse CSV data - expect comma-separated format
    const lines = csvData.trim().split('\n');
    const headers = lines[0].split(',').map(h => h.trim());
    
    console.log(`📊 Processing ${lines.length - 1} data rows with headers:`, headers);
    
    const transformedData = [];
    
    // Get store mapping for UUID lookup
    console.log('🏪 Fetching store mappings from Supabase...');
    const { data: stores, error: storeError } = await supabase
      .from('stores')
      .select('store_id, store_number, store_name');
    
    if (storeError) {
      console.error('❌ Store lookup error:', storeError);
      return res.status(500).json({
        error: 'Failed to fetch store mappings',
        details: storeError.message,
        timestamp: new Date().toISOString()
      });
    }
    
    // Create store lookup maps
    const storeByNumber = new Map();
    const storeByName = new Map();
    stores.forEach(store => {
      if (store.store_number) storeByNumber.set(store.store_number.toString(), store.store_id);
      if (store.store_name) storeByName.set(store.store_name.toLowerCase(), store.store_id);
    });
    
    // Process each data row (skip header)
    for (let i = 1; i < lines.length; i++) {
      const values = lines[i].split(',').map(v => v.trim());
      const rowData = {};
      
      // Map values to headers
      headers.forEach((header, index) => {
        rowData[header.toLowerCase()] = values[index];
      });
      
      // Find store_id - try store number first, then store name
      let storeId = null;
      
      if (rowData.store_number || rowData['store number'] || rowData.store) {
        const storeNum = rowData.store_number || rowData['store number'] || rowData.store;
        storeId = storeByNumber.get(storeNum.toString());
      }
      
      if (!storeId && (rowData.store_name || rowData['store name'])) {
        const storeName = (rowData.store_name || rowData['store name']).toLowerCase();
        storeId = storeByName.get(storeName);
      }
      
      if (!storeId) {
        console.warn(`⚠️ Could not find store_id for row ${i}:`, rowData);
        continue;
      }
      
      // Transform SMG data to daily_cx_scores format
      // Look for question/score columns (typical SMG format)
      Object.keys(rowData).forEach(key => {
        if (key.includes('question') || key.includes('q_') || key.includes('score')) {
          // Extract question and score data
          const questionMatch = key.match(/q(?:uestion)?[_\s]*(\d+|overall|satisfaction)/i);
          if (questionMatch && rowData[key] && rowData[key] !== '') {
            
            // Determine question text
            let questionText = questionMatch[1];
            if (questionText === 'overall') questionText = 'Overall Satisfaction';
            else if (questionText === 'satisfaction') questionText = 'Satisfaction Score';
            else questionText = `Question ${questionText}`;
            
            // Parse score value
            const scoreValue = parseFloat(rowData[key]);
            if (isNaN(scoreValue) || scoreValue < 1 || scoreValue > 5) {
              console.warn(`⚠️ Invalid score value for ${questionText}:`, rowData[key]);
              return;
            }
            
            // Create transformed record
            const transformedRecord = {
              store_id: storeId,
              date: date,
              question: questionText,
              score: Math.round(scoreValue),
              response_count: rowData.response_count ? parseInt(rowData.response_count) : 1,
              response_percent: rowData.response_percent ? parseFloat(rowData.response_percent) : null,
              total_responses: rowData.total_responses ? parseInt(rowData.total_responses) : null
            };
            
            transformedData.push(transformedRecord);
          }
        }
      });
    }
    
    const result = {
      success: true,
      original_rows: lines.length - 1,
      transformed_rows: transformedData.length,
      stores_found: stores.length,
      transformation_method: 'multi_year_transformer_logic',
      data: transformedData,
      timestamp: new Date().toISOString()
    };
    
    console.log(`✅ SMG CSV transformation complete: ${transformedData.length} records`);
    res.json(result);
    
  } catch (error) {
    console.error('❌ SMG transformation error:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// MODULE 3: SMG Upload - Supabase upload using upload.js patterns
app.post('/smg-upload', async (req, res) => {
  try {
    console.log('📤 Starting SMG data upload to Supabase...');
    
    const { data, mode = 'upsert' } = req.body;
    
    if (!data || !Array.isArray(data)) {
      return res.status(400).json({
        error: 'Missing or invalid parameter: data (must be array)',
        timestamp: new Date().toISOString()
      });
    }
    
    if (data.length === 0) {
      return res.status(400).json({
        error: 'Empty data array provided',
        timestamp: new Date().toISOString()
      });
    }
    
    console.log(`📊 Uploading ${data.length} records using ${mode} mode...`);
    
    // Validate data structure
    const requiredFields = ['store_id', 'date', 'question', 'score'];
    const validationErrors = [];
    
    data.forEach((record, index) => {
      requiredFields.forEach(field => {
        if (!record[field]) {
          validationErrors.push(`Record ${index}: Missing required field '${field}'`);
        }
      });
      
      // Validate score range
      if (record.score && (record.score < 1 || record.score > 5)) {
        validationErrors.push(`Record ${index}: Score must be between 1 and 5, got ${record.score}`);
      }
    });
    
    if (validationErrors.length > 0) {
      return res.status(400).json({
        error: 'Data validation failed',
        validation_errors: validationErrors,
        timestamp: new Date().toISOString()
      });
    }
    
    let uploadResult;
    let uploadStats = {
      inserted: 0,
      updated: 0,
      errors: 0,
      total: data.length
    };
    
    if (mode === 'upsert') {
      // Use upsert for handling duplicates (update if exists, insert if new)
      console.log('🔄 Using UPSERT mode for conflict resolution...');
      
      const { data: upsertData, error: upsertError } = await supabase
        .from('daily_cx_scores')
        .upsert(data, {
          onConflict: 'store_id,date,question,score',
          ignoreDuplicates: false
        });
      
      if (upsertError) {
        console.error('❌ Upsert error:', upsertError);
        return res.status(500).json({
          error: 'Database upsert failed',
          details: upsertError.message,
          timestamp: new Date().toISOString()
        });
      }
      
      uploadResult = upsertData;
      uploadStats.inserted = data.length; // Upsert doesn't provide granular stats
      
    } else if (mode === 'insert') {
      // Use insert mode (will fail on duplicates)
      console.log('📥 Using INSERT mode...');
      
      const { data: insertData, error: insertError } = await supabase
        .from('daily_cx_scores')
        .insert(data);
      
      if (insertError) {
        console.error('❌ Insert error:', insertError);
        return res.status(500).json({
          error: 'Database insert failed',
          details: insertError.message,
          timestamp: new Date().toISOString()
        });
      }
      
      uploadResult = insertData;
      uploadStats.inserted = data.length;
      
    } else if (mode === 'replace') {
      // Delete existing records for the same date/stores, then insert new ones
      console.log('🔄 Using REPLACE mode - deleting existing records...');
      
      const dates = [...new Set(data.map(d => d.date))];
      const storeIds = [...new Set(data.map(d => d.store_id))];
      
      // Delete existing records
      const { error: deleteError } = await supabase
        .from('daily_cx_scores')
        .delete()
        .in('date', dates)
        .in('store_id', storeIds);
      
      if (deleteError) {
        console.error('❌ Delete error:', deleteError);
        return res.status(500).json({
          error: 'Database delete failed during replace mode',
          details: deleteError.message,
          timestamp: new Date().toISOString()
        });
      }
      
      // Insert new records
      const { data: insertData, error: insertError } = await supabase
        .from('daily_cx_scores')
        .insert(data);
      
      if (insertError) {
        console.error('❌ Insert error after delete:', insertError);
        return res.status(500).json({
          error: 'Database insert failed during replace mode',
          details: insertError.message,
          timestamp: new Date().toISOString()
        });
      }
      
      uploadResult = insertData;
      uploadStats.inserted = data.length;
    }
    
    // Get upload summary
    const uniqueDates = [...new Set(data.map(d => d.date))];
    const uniqueStores = [...new Set(data.map(d => d.store_id))];
    const uniqueQuestions = [...new Set(data.map(d => d.question))];
    
    const result = {
      success: true,
      upload_mode: mode,
      statistics: uploadStats,
      summary: {
        dates_affected: uniqueDates.length,
        stores_affected: uniqueStores.length,
        questions_processed: uniqueQuestions.length,
        date_range: uniqueDates.sort(),
        records_processed: data.length
      },
      upload_method: 'supabase_bulk_insert',
      timestamp: new Date().toISOString()
    };
    
    console.log(`✅ SMG data upload complete: ${data.length} records processed`);
    res.json(result);
    
  } catch (error) {
    console.error('❌ SMG upload error:', error);
    res.status(500).json({
      error: 'Internal server error',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Start server
app.listen(PORT, () => {
  console.log(`🚀 SMG Cloud Automation Pipeline running on port ${PORT}`);
  console.log('Environment variables loaded:');
  console.log('- SUPABASE_URL:', process.env.SUPABASE_URL ? 'Set' : 'Missing');
  console.log('- SUPABASE_ANON_KEY:', process.env.SUPABASE_ANON_KEY ? 'Set' : 'Missing');
});

module.exports = app;