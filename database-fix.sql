-- Quick fix for aggregate permissions issue
-- Remove count() queries that are causing permission errors in status module

-- This is a documentation file for the fix applied to Module 5
-- Changed from count() queries to simple select queries to avoid aggregate function restrictions