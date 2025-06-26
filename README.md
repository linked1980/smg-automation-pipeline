# SMG Cloud Automation Pipeline - Phase 2 ✅ COMPLETE

## Overview
Phase 2 modular pipeline components for SMG data automation, built to replace OneDrive workflow with cloud-native automation.

## 🚀 **ALL MODULES BUILT & READY FOR TESTING**

### ✅ Module 1: `/smg-daily-dates`
- **Function**: 3 calendar day calculation (no weekends/holidays filtering)
- **Method**: GET
- **Input**: None (uses current date)
- **Output**: Array of 3 business dates
- **Example**: `["2025-06-26", "2025-06-25", "2025-06-24"]`

### ✅ Module 2: `/smg-transform` 
- **Function**: CSV transformation using multi-year-transformer.js logic
- **Method**: POST
- **Input**: `{ csvData: "csv_string", date: "YYYY-MM-DD" }`
- **Features**: Store UUID mapping, question/score extraction, data validation
- **Output**: Transformed data ready for `daily_cx_scores` table

### ✅ Module 3: `/smg-upload`
- **Function**: Supabase upload using upload.js patterns
- **Method**: POST
- **Input**: `{ data: [...], mode: "upsert|insert|replace" }`
- **Features**: Bulk upload, conflict resolution, validation
- **Output**: Upload statistics and summary

### ✅ Module 4: `/smg-pipeline`
- **Function**: Complete integration flow (chains modules 1-3)
- **Method**: POST
- **Input**: `{ csvData: "csv_string", dates?: [...], uploadMode?: "upsert" }`
- **Features**: End-to-end processing, stage tracking, error handling
- **Output**: Detailed pipeline execution results

### ✅ Module 5: `/smg-status`
- **Function**: Comprehensive system monitoring
- **Method**: GET
- **Input**: None
- **Features**: Health checks, database connectivity, data summary, performance metrics
- **Output**: Complete system status report

## 📋 API Reference

### Health Check
```bash
GET /
```

### Module Endpoints
```bash
# Get processing dates
GET /smg-daily-dates

# Transform CSV data
POST /smg-transform
{
  "csvData": "store,question_1,question_2\n123,4.5,3.8\n124,4.2,4.1",
  "date": "2025-06-26"
}

# Upload transformed data
POST /smg-upload
{
  "data": [{"store_id": "uuid", "date": "2025-06-26", "question": "Overall", "score": 4}],
  "mode": "upsert"
}

# Run complete pipeline
POST /smg-pipeline
{
  "csvData": "store,question_1\n123,4.5\n124,4.2",
  "uploadMode": "upsert"
}

# Check system status
GET /smg-status
```

## 🔧 Environment Variables
```env
SUPABASE_URL=your_supabase_url
SUPABASE_ANON_KEY=your_supabase_anon_key
PORT=8080
```

## 🗄️ Database Dependencies
- **stores** table: `store_id`, `store_number`, `store_name`
- **daily_cx_scores** table: `store_id`, `date`, `question`, `score`, `response_count`, `response_percent`, `total_responses`
- **calendar** table: `date`, `is_weekend`, `is_holiday` (used for reference)

## 🚀 Deployment
- **Repository**: `linked1980/smg-automation-pipeline`
- **Railway Service**: `smg-pipeline-phase2`
- **Domain**: `smg-pipeline-phase2-production.up.railway.app`

## 📊 Testing Status
**Phase 2 modules built while user away - ready for individual component testing upon return.**

### Test Sequence Recommendation:
1. Test `/smg-daily-dates` - Verify date calculation
2. Test `/smg-transform` with sample CSV - Verify transformation logic
3. Test `/smg-upload` with transformed data - Verify database upload
4. Test `/smg-pipeline` end-to-end - Verify complete flow
5. Test `/smg-status` - Verify monitoring capabilities

## 🔄 Integration with Phase 1
- **Phase 1 Endpoints**: `/smg-download`, `/smg-backfill` 
- **Phase 2 Endpoints**: All 5 modules above
- **Integration**: Phase 1 provides CSV data → Phase 2 processes and uploads

## 📈 Next Steps
1. **Testing**: Individual module validation
2. **Integration**: Connect with Phase 1 SMG download endpoints
3. **Automation**: Schedule pipeline execution (cron/scheduled tasks)
4. **Monitoring**: Set up alerts based on `/smg-status` health checks

---
**Built**: June 26, 2025  
**Status**: ✅ All 5 modules complete and ready for testing