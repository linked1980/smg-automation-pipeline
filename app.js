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

// TEST PAGE ENDPOINT
app.get('/test', (req, res) => {
  res.send(`
<!DOCTYPE html>
<html>
<head>
    <title>SMG Pipeline Tester</title>
    <style>
        body { font-family: Arial, sans-serif; margin: 20px; background: #f9f9f9; }
        .module { border: 1px solid #ccc; padding: 20px; margin: 10px 0; background: white; border-radius: 5px; }
        button { background: #007cba; color: white; padding: 10px 20px; border: none; cursor: pointer; border-radius: 3px; }
        button:hover { background: #005a87; }
        .result { background: #f5f5f5; padding: 10px; margin: 10px 0; white-space: pre-wrap; font-family: monospace; border: 1px solid #ddd; max-height: 300px; overflow-y: auto; }
        textarea { width: 100%; height: 80px; font-family: monospace; }
        input, select { padding: 5px; }
        .status { padding: 10px; margin: 10px 0; border-radius: 3px; }
        .success { background: #d4edda; color: #155724; border: 1px solid #c3e6cb; }
        .error { background: #f8d7da; color: #721c24; border: 1px solid #f5c6cb; }
    </style>
</head>
<body>
    <h1>🚀 SMG Pipeline Tester</h1>
    <div class="status success">✅ Module 1 working! Test the other modules below:</div>
    
    <div class="module">
        <h3>📊 Module 2: Transform CSV Data</h3>
        <p>Test CSV transformation logic:</p>
        <label>CSV Data:</label>
        <textarea id="csvData">store,question_1,question_2
123,4.5,3.8
124,4.2,4.1</textarea>
        <br><br>
        <label>Date:</label> <input type="date" id="transformDate" value="2025-06-26">
        <br><br>
        <button onclick="testTransform()">🔄 Test Transform</button>
        <div id="transformResult" class="result">Click button to test...</div>
    </div>
    
    <div class="module">
        <h3>🚀 Module 4: Complete Pipeline</h3>
        <p>Test end-to-end pipeline (dates → transform → upload):</p>
        <label>CSV Data:</label>
        <textarea id="pipelineCsv">store,question_1
123,4.5
124,4.2</textarea>
        <br><br>
        <label>Upload Mode:</label> 
        <select id="uploadMode">
            <option value="upsert">Upsert (recommended)</option>
            <option value="insert">Insert</option>
            <option value="replace">Replace</option>
        </select>
        <br><br>
        <button onclick="testPipeline()">🚀 Test Full Pipeline</button>
        <div id="pipelineResult" class="result">Click button to test...</div>
    </div>

    <div class="module">
        <h3>📋 Quick Links</h3>
        <p>
            <a href="/" target="_blank">Health Check</a> | 
            <a href="/smg-daily-dates" target="_blank">Module 1: Daily Dates</a> | 
            <a href="/smg-status" target="_blank">Module 5: System Status</a>
        </p>
    </div>

    <script>
        const baseUrl = window.location.origin;
        
        async function testTransform() {
            const csvData = document.getElementById('csvData').value;
            const date = document.getElementById('transformDate').value;
            const resultDiv = document.getElementById('transformResult');
            
            resultDiv.textContent = '🔄 Testing transform...';
            resultDiv.className = 'result';
            
            try {
                const response = await fetch(baseUrl + '/smg-transform', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ csvData, date })
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    resultDiv.textContent = '✅ Transform Success:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#d4edda';
                } else {
                    resultDiv.textContent = '❌ Transform Error:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#f8d7da';
                }
            } catch (error) {
                resultDiv.textContent = '❌ Network Error: ' + error.message;
                resultDiv.style.background = '#f8d7da';
            }
        }
        
        async function testPipeline() {
            const csvData = document.getElementById('pipelineCsv').value;
            const uploadMode = document.getElementById('uploadMode').value;
            const resultDiv = document.getElementById('pipelineResult');
            
            resultDiv.textContent = '🚀 Running full pipeline...\\nThis may take a few seconds...';
            resultDiv.className = 'result';
            
            try {
                const response = await fetch(baseUrl + '/smg-pipeline', {
                    method: 'POST',
                    headers: { 'Content-Type': 'application/json' },
                    body: JSON.stringify({ csvData, uploadMode })
                });
                
                const result = await response.json();
                
                if (response.ok) {
                    resultDiv.textContent = '✅ Pipeline Success:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#d4edda';
                } else {
                    resultDiv.textContent = '❌ Pipeline Error:\\n\\n' + JSON.stringify(result, null, 2);
                    resultDiv.style.background = '#f8d7da';
                }
            } catch (error) {
                resultDiv.textContent = '❌ Network Error: ' + error.message;
                resultDiv.style.background = '#f8d7da';
            }
        }
    </script>
</body>
</html>
  `);
});

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
      '/smg-pipeline ✅',
      '/smg-status ✅'
    ],
    test_page: '/test',
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

// MODULE 4: SMG Pipeline - Complete integration flow
app.post('/smg-pipeline', async (req, res) => {
  try {
    console.log('🚀 Starting SMG complete pipeline execution...');
    
    const { csvData, dates, uploadMode = 'upsert' } = req.body;
    const pipelineStart = new Date();
    const pipelineResults = {
      pipeline_id: `pipeline_${Date.now()}`,
      status: 'running',
      started_at: pipelineStart.toISOString(),
      stages: {
        date_calculation: { status: 'pending', duration_ms: 0 },
        transformation: { status: 'pending', duration_ms: 0 },
        upload: { status: 'pending', duration_ms: 0 }
      },
      total_duration_ms: 0,
      records_processed: 0,
      errors: []
    };
    
    // STAGE 1: Date calculation (if dates not provided)
    let processingDates = dates;
    if (!processingDates || !Array.isArray(processingDates)) {
      console.log('📅 Stage 1: Calculating processing dates...');
      const stage1Start = Date.now();
      
      try {
        const today = new Date();
        processingDates = [];
        
        // Calculate the last 3 calendar days
        for (let i = 0; i < 3; i++) {
          const date = new Date(today);
          date.setDate(today.getDate() - i);
          processingDates.push(date.toISOString().split('T')[0]);
        }
        
        pipelineResults.stages.date_calculation = {
          status: 'completed',
          duration_ms: Date.now() - stage1Start,
          dates_calculated: processingDates
        };
        
        console.log(`✅ Stage 1 complete: ${processingDates.length} dates calculated`);
        
      } catch (error) {
        pipelineResults.stages.date_calculation = {
          status: 'failed',
          duration_ms: Date.now() - stage1Start,
          error: error.message
        };
        pipelineResults.errors.push(`Date calculation failed: ${error.message}`);
        throw error;
      }
    } else {
      pipelineResults.stages.date_calculation = {
        status: 'skipped',
        duration_ms: 0,
        reason: 'dates_provided_by_user'
      };
    }
    
    // Validate CSV data is provided
    if (!csvData) {
      throw new Error('CSV data is required for pipeline execution');
    }
    
    // STAGE 2: Data transformation for each date
    console.log('🔄 Stage 2: Transforming CSV data...');
    const stage2Start = Date.now();
    let allTransformedData = [];
    
    try {
      for (const processDate of processingDates) {
        console.log(`  Processing date: ${processDate}`);
        
        // Parse CSV data - expect comma-separated format
        const lines = csvData.trim().split('\n');
        const headers = lines[0].split(',').map(h => h.trim());
        
        const transformedData = [];
        
        // Get store mapping for UUID lookup
        const { data: stores, error: storeError } = await supabase
          .from('stores')
          .select('store_id, store_number, store_name');
        
        if (storeError) {
          throw new Error(`Store lookup failed: ${storeError.message}`);
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
          
          // Find store_id
          let storeId = null;
          if (rowData.store_number || rowData['store number'] || rowData.store) {
            const storeNum = rowData.store_number || rowData['store number'] || rowData.store;
            storeId = storeByNumber.get(storeNum.toString());
          }
          
          if (!storeId && (rowData.store_name || rowData['store name'])) {
            const storeName = (rowData.store_name || rowData['store name']).toLowerCase();
            storeId = storeByName.get(storeName);
          }
          
          if (!storeId) continue;
          
          // Transform SMG data to daily_cx_scores format
          Object.keys(rowData).forEach(key => {
            if (key.includes('question') || key.includes('q_') || key.includes('score')) {
              const questionMatch = key.match(/q(?:uestion)?[_\s]*(\d+|overall|satisfaction)/i);
              if (questionMatch && rowData[key] && rowData[key] !== '') {
                
                let questionText = questionMatch[1];
                if (questionText === 'overall') questionText = 'Overall Satisfaction';
                else if (questionText === 'satisfaction') questionText = 'Satisfaction Score';
                else questionText = `Question ${questionText}`;
                
                const scoreValue = parseFloat(rowData[key]);
                if (isNaN(scoreValue) || scoreValue < 1 || scoreValue > 5) return;
                
                const transformedRecord = {
                  store_id: storeId,
                  date: processDate,
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
        
        allTransformedData = allTransformedData.concat(transformedData);
      }
      
      pipelineResults.stages.transformation = {
        status: 'completed',
        duration_ms: Date.now() - stage2Start,
        original_rows: csvData.trim().split('\n').length - 1,
        transformed_rows: allTransformedData.length,
        dates_processed: processingDates.length
      };
      
      console.log(`✅ Stage 2 complete: ${allTransformedData.length} records transformed`);
      
    } catch (error) {
      pipelineResults.stages.transformation = {
        status: 'failed',
        duration_ms: Date.now() - stage2Start,
        error: error.message
      };
      pipelineResults.errors.push(`Transformation failed: ${error.message}`);
      throw error;
    }
    
    // STAGE 3: Upload to Supabase
    console.log('📤 Stage 3: Uploading to Supabase...');
    const stage3Start = Date.now();
    
    try {
      if (allTransformedData.length === 0) {
        throw new Error('No data to upload after transformation');
      }
      
      // Validate data structure
      const requiredFields = ['store_id', 'date', 'question', 'score'];
      const validationErrors = [];
      
      allTransformedData.forEach((record, index) => {
        requiredFields.forEach(field => {
          if (!record[field]) {
            validationErrors.push(`Record ${index}: Missing required field '${field}'`);
          }
        });
      });
      
      if (validationErrors.length > 0) {
        throw new Error(`Data validation failed: ${validationErrors.join(', ')}`);
      }
      
      // Perform upload based on mode
      let uploadResult;
      if (uploadMode === 'upsert') {
        const { data: upsertData, error: upsertError } = await supabase
          .from('daily_cx_scores')
          .upsert(allTransformedData, {
            onConflict: 'store_id,date,question,score',
            ignoreDuplicates: false
          });
        
        if (upsertError) throw new Error(`Upsert failed: ${upsertError.message}`);
        uploadResult = upsertData;
        
      } else if (uploadMode === 'replace') {
        const dates = [...new Set(allTransformedData.map(d => d.date))];
        const storeIds = [...new Set(allTransformedData.map(d => d.store_id))];
        
        // Delete existing records
        const { error: deleteError } = await supabase
          .from('daily_cx_scores')
          .delete()
          .in('date', dates)
          .in('store_id', storeIds);
        
        if (deleteError) throw new Error(`Delete failed: ${deleteError.message}`);
        
        // Insert new records
        const { data: insertData, error: insertError } = await supabase
          .from('daily_cx_scores')
          .insert(allTransformedData);
        
        if (insertError) throw new Error(`Insert failed: ${insertError.message}`);
        uploadResult = insertData;
      }
      
      const uniqueDates = [...new Set(allTransformedData.map(d => d.date))];
      const uniqueStores = [...new Set(allTransformedData.map(d => d.store_id))];
      
      pipelineResults.stages.upload = {
        status: 'completed',
        duration_ms: Date.now() - stage3Start,
        records_uploaded: allTransformedData.length,
        upload_mode: uploadMode,
        dates_affected: uniqueDates.length,
        stores_affected: uniqueStores.length
      };
      
      console.log(`✅ Stage 3 complete: ${allTransformedData.length} records uploaded`);
      
    } catch (error) {
      pipelineResults.stages.upload = {
        status: 'failed',
        duration_ms: Date.now() - stage3Start,
        error: error.message
      };
      pipelineResults.errors.push(`Upload failed: ${error.message}`);
      throw error;
    }
    
    // Pipeline completion
    pipelineResults.status = 'completed';
    pipelineResults.completed_at = new Date().toISOString();
    pipelineResults.total_duration_ms = Date.now() - pipelineStart.getTime();
    pipelineResults.records_processed = allTransformedData.length;
    
    console.log(`🎉 SMG Pipeline complete: ${allTransformedData.length} records processed in ${pipelineResults.total_duration_ms}ms`);
    
    res.json({
      success: true,
      pipeline_results: pipelineResults,
      timestamp: new Date().toISOString()
    });
    
  } catch (error) {
    console.error('❌ SMG pipeline error:', error);
    
    pipelineResults.status = 'failed';
    pipelineResults.completed_at = new Date().toISOString();
    pipelineResults.total_duration_ms = Date.now() - pipelineStart.getTime();
    pipelineResults.final_error = error.message;
    
    res.status(500).json({
      success: false,
      pipeline_results: pipelineResults,
      error: 'Pipeline execution failed',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// MODULE 5: SMG Status - Monitoring and management
app.get('/smg-status', async (req, res) => {
  try {
    console.log('📊 Checking SMG system status...');
    const statusStart = Date.now();
    
    const systemStatus = {
      overall_status: 'healthy',
      timestamp: new Date().toISOString(),
      uptime_seconds: process.uptime(),
      system_checks: {},
      database_health: {},
      data_summary: {},
      configuration: {},
      performance_metrics: {}
    };
    
    // SYSTEM HEALTH CHECKS
    console.log('🔍 Running system health checks...');
    
    // Check environment variables
    systemStatus.system_checks.environment_variables = {
      status: 'healthy',
      required_vars: requiredEnvVars.map(varName => ({
        name: varName,
        configured: !!process.env[varName]
      })),
      missing_vars: missingEnvVars
    };
    
    // Check memory usage
    const memUsage = process.memoryUsage();
    systemStatus.system_checks.memory = {
      status: memUsage.heapUsed / memUsage.heapTotal < 0.9 ? 'healthy' : 'warning',
      heap_used_mb: Math.round(memUsage.heapUsed / 1024 / 1024),
      heap_total_mb: Math.round(memUsage.heapTotal / 1024 / 1024),
      heap_usage_percent: Math.round((memUsage.heapUsed / memUsage.heapTotal) * 100)
    };
    
    // DATABASE HEALTH CHECKS
    console.log('🗄️ Running database health checks...');
    
    try {
      // Test basic connectivity with simple queries (no aggregates)
      const connectivityTest = Date.now();
      const { data: healthTest, error: healthError } = await supabase
        .from('stores')
        .select('store_id')
        .limit(1);
      
      const connectivityTime = Date.now() - connectivityTest;
      
      if (healthError) {
        systemStatus.database_health.connectivity = {
          status: 'failed',
          error: healthError.message,
          response_time_ms: connectivityTime
        };
        systemStatus.overall_status = 'degraded';
      } else {
        systemStatus.database_health.connectivity = {
          status: 'healthy',
          response_time_ms: connectivityTime
        };
      }
      
      // Check table accessibility
      const tablesCheck = Date.now();
      const tableResults = await Promise.allSettled([
        supabase.from('stores').select('store_id').limit(1),
        supabase.from('daily_cx_scores').select('store_id').limit(1),
        supabase.from('calendar').select('date').limit(1)
      ]);
      
      systemStatus.database_health.table_accessibility = {
        status: tableResults.every(r => r.status === 'fulfilled') ? 'healthy' : 'warning',
        response_time_ms: Date.now() - tablesCheck,
        tables: {
          stores: tableResults[0].status === 'fulfilled' ? 'accessible' : 'error',
          daily_cx_scores: tableResults[1].status === 'fulfilled' ? 'accessible' : 'error',
          calendar: tableResults[2].status === 'fulfilled' ? 'accessible' : 'error'
        }
      };
      
    } catch (dbError) {
      systemStatus.database_health.connectivity = {
        status: 'failed',
        error: dbError.message
      };
      systemStatus.overall_status = 'degraded';
    }
    
    // DATA SUMMARY (using simple queries)
    console.log('📈 Gathering data summary...');
    
    try {
      const summaryQueries = await Promise.allSettled([
        supabase.from('stores').select('store_id').limit(100),
        supabase.from('daily_cx_scores').select('date').order('date', { ascending: false }).limit(1),
        supabase.from('daily_cx_scores').select('date').order('date', { ascending: true }).limit(1)
      ]);
      
      systemStatus.data_summary = {
        stores_count: summaryQueries[0].status === 'fulfilled' ? 
          (summaryQueries[0].value.data?.length || 0) : 'unknown',
        latest_date: summaryQueries[1].status === 'fulfilled' ? 
          (summaryQueries[1].value.data?.[0]?.date || 'unknown') : 'unknown',
        earliest_date: summaryQueries[2].status === 'fulfilled' ? 
          (summaryQueries[2].value.data?.[0]?.date || 'unknown') : 'unknown'
      };
      
      // Calculate data freshness
      if (systemStatus.data_summary.latest_date !== 'unknown') {
        const latestDate = new Date(systemStatus.data_summary.latest_date);
        const today = new Date();
        const daysDiff = Math.floor((today - latestDate) / (1000 * 60 * 60 * 24));
        
        systemStatus.data_summary.data_freshness = {
          latest_data_age_days: daysDiff,
          status: daysDiff <= 7 ? 'fresh' : daysDiff <= 30 ? 'stale' : 'very_stale'
        };
      }
      
    } catch (summaryError) {
      systemStatus.data_summary.error = summaryError.message;
    }
    
    // CONFIGURATION STATUS
    systemStatus.configuration = {
      node_version: process.version,
      platform: process.platform,
      port: PORT,
      environment: process.env.NODE_ENV || 'production',
      modules_available: [
        { name: 'smg-daily-dates', method: 'GET', status: 'active' },
        { name: 'smg-transform', method: 'POST', status: 'active' },
        { name: 'smg-upload', method: 'POST', status: 'active' },
        { name: 'smg-pipeline', method: 'POST', status: 'active' },
        { name: 'smg-status', method: 'GET', status: 'active' }
      ]
    };
    
    // PERFORMANCE METRICS
    systemStatus.performance_metrics = {
      status_check_duration_ms: Date.now() - statusStart,
      uptime_formatted: formatUptime(process.uptime()),
      memory_usage: systemStatus.system_checks.memory,
      database_response_time_ms: systemStatus.database_health.connectivity?.response_time_ms || null
    };
    
    // DETERMINE OVERALL STATUS
    if (systemStatus.database_health.connectivity?.status === 'failed') {
      systemStatus.overall_status = 'critical';
    } else if (systemStatus.database_health.connectivity?.status === 'warning' || 
               systemStatus.system_checks.memory.status === 'warning') {
      systemStatus.overall_status = 'warning';
    }
    
    console.log(`✅ SMG status check complete: ${systemStatus.overall_status}`);
    res.json(systemStatus);
    
  } catch (error) {
    console.error('❌ SMG status check error:', error);
    res.status(500).json({
      overall_status: 'critical',
      error: 'Status check failed',
      message: error.message,
      timestamp: new Date().toISOString()
    });
  }
});

// Helper function to format uptime
function formatUptime(seconds) {
  const days = Math.floor(seconds / 86400);
  const hours = Math.floor((seconds % 86400) / 3600);
  const minutes = Math.floor((seconds % 3600) / 60);
  const secs = Math.floor(seconds % 60);
  
  if (days > 0) return `${days}d ${hours}h ${minutes}m ${secs}s`;
  if (hours > 0) return `${hours}h ${minutes}m ${secs}s`;
  if (minutes > 0) return `${minutes}m ${secs}s`;
  return `${secs}s`;
}

// Start server
app.listen(PORT, () => {
  console.log(`🚀 SMG Cloud Automation Pipeline running on port ${PORT}`);
  console.log('Environment variables loaded:');
  console.log('- SUPABASE_URL:', process.env.SUPABASE_URL ? 'Set' : 'Missing');
  console.log('- SUPABASE_ANON_KEY:', process.env.SUPABASE_ANON_KEY ? 'Set' : 'Missing');
});

module.exports = app;