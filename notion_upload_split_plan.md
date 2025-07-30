# Notion Uploader: File Splitting, Metadata, and Database Integration Plan

## 1. Update User Database Schema
- **File:** `uploader/notion_uploader.py:create_user_database()`
- **Action:**
  - Add one new properties to the Notion user database:
    - `is_visible` (checkbox)

## 2. Upload Flow Adjustments


### 2.1. File Size Check and Splitting Logic
- **Splitting threshold:** 50 MiB (52,428,800 bytes)
- **Upload type threshold:** 20 MiB (20,971,520 bytes)

- **If file > 50 MiB:**
  1. **Split file** into parts, each ≤50 MiB (e.g., 60 MiB → 50 MiB part + 10 MiB part).
  2. **Upload each part:**
     - If part > 20 MiB: upload as multipart.
     - If part ≤ 20 MiB: upload as single-part.
     - For each part, create a DB entry:
       - `is_visible`: unchecked
       - `file`: set to the uploaded part
  3. **After all parts uploaded:**
     - Create a JSON metadata file (`file.json`) describing the parts (order, IDs, hashes, permanent download link, etc).
     - Upload the JSON file.
     - Create a DB entry:
       - `is_visible`: checked
       - `file`: set to the JSON file

  **Example:**
  - For a 60 MiB file:
    - Split into 50 MiB part and 10 MiB part
    - 50 MiB part → upload as multipart
    - 10 MiB part → upload as single-part
    - Each part gets its own DB entry with `is_visible` unchecked
    - After all parts, upload a JSON metadata file and create a DB entry with `is_visible` checked

- **If file ≤ 50 MiB:**
  - If file > 20 MiB: upload as multipart
  - If file ≤ 20 MiB: upload as single-part
  - Create a single DB entry:
    - `is_visible`: checked
    - `file`: set to the file
    - No JSON metadata needed

### 2.2. Code Locations
- **File splitting and upload logic:**
  - `uploader/streaming_uploader.py` (main upload flow)
  - `uploader/parallel_processor.py` (chunked/multipart upload)

## 3. Download Logic
- **If file was split:**
  - Download the JSON metadata file
  - Use metadata to download all parts
  - Reconstruct the original file from parts
- **If file was not split:**
  - Download directly from the DB entry

## 4. Implementation Steps
1. Update `create_user_database()` to add new properties
2. Refactor upload logic to:
   - Check file size
   - Split/upload parts if needed
   - Create DB entries for each part
   - Create/upload JSON metadata and DB entry
3. Refactor download logic to support reconstruction from parts
4. Test both split and non-split upload/download flows
