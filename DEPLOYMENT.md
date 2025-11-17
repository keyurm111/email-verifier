# Render Deployment Guide

## Fixed Issues

The 502 error was caused by several issues that have now been fixed:

### 1. Missing Dependencies
- Added `email-validator` (was imported but not in requirements.txt)
- Added `openpyxl` (required for Excel file writing)
- Removed `smtplib` (it's a built-in Python module)

### 2. Render Configuration
- Created `Procfile` for proper Streamlit deployment on Render
- Added `.streamlit/config.toml` with proper timeout and upload settings

### 3. Performance Optimizations
- Reduced `MAX_THREADS` from 5 to 3 (to avoid overwhelming Render's resources)
- Reduced SMTP timeout from 5s to 3s (faster processing)
- Reduced DNS timeout from 5s to 3s (faster processing)

### 4. File Handling
- Changed file output to use `temp/` directory for better organization
- Added proper error handling

## Important Notes

⚠️ **Request Timeout Limitation**: Render has a request timeout limit (typically 30 seconds). If you're processing very large CSV files (hundreds or thousands of emails), the process might still timeout. 

**Solutions for large files:**
1. Process files in smaller batches
2. Consider upgrading your Render plan for longer timeouts
3. Use a background job service for very large files

## Deployment Steps

1. Push all changes to your repository
2. On Render dashboard:
   - Go to your service settings
   - Ensure the build command is: `pip install -r requirements.txt`
   - Ensure the start command uses the Procfile (should be automatic)
3. Redeploy your service

## Testing

After deployment, test with a small CSV file first (10-20 emails) to ensure everything works before processing larger files.

