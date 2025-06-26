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
      '/smg-daily-dates',
      '/smg-transform', 
      '/smg-upload',
      '/smg-pipeline',
      '/smg-status'
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

// Start server
app.listen(PORT, () => {
  console.log(`🚀 SMG Cloud Automation Pipeline running on port ${PORT}`);
  console.log('Environment variables loaded:');
  console.log('- SUPABASE_URL:', process.env.SUPABASE_URL ? 'Set' : 'Missing');
  console.log('- SUPABASE_ANON_KEY:', process.env.SUPABASE_ANON_KEY ? 'Set' : 'Missing');
});

module.exports = app;