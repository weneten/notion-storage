#!/usr/bin/env python3
"""
Diagnostic script to investigate missing buttons after file upload.
This will help confirm our assumptions about the root cause.
"""

import json

def add_frontend_diagnostics():
    """
    Add diagnostic logs to the frontend JavaScript to track the issue
    """
    
    # Diagnostic JavaScript to add to streaming-upload.js
    diagnostic_js = '''
// ========== DIAGNOSTIC LOGS FOR MISSING BUTTONS ISSUE ==========

// Override the loadFiles function to add diagnostic logging
const originalLoadFiles = loadFiles;
async function loadFiles() {
    console.log("🔍 DIAGNOSTIC: loadFiles() called");
    
    try {
        console.log('🔍 DIAGNOSTIC: Fetching file list from /api/files...');
        const response = await fetch('/api/files');
        if (!response.ok) {
            throw new Error('Failed to fetch file list');
        }

        const data = await response.json();
        console.log('🔍 DIAGNOSTIC: API Response received:', data);
        
        if (!data.files) {
            throw new Error('Invalid response format');
        }

        console.log('🔍 DIAGNOSTIC: Files array length:', data.files.length);
        console.log('🔍 DIAGNOSTIC: First file structure:', data.files[0]);
        
        // Check what properties each file has
        data.files.forEach((file, index) => {
            console.log(`🔍 DIAGNOSTIC: File ${index + 1} properties:`, {
                id: file.id,
                name: file.name,
                size: file.size,
                file_hash: file.file_hash,
                is_public: file.is_public,
                has_toggle_data: !!(file.id && file.file_hash !== undefined && file.is_public !== undefined)
            });
        });

        const filesContainer = document.getElementById('files-container');
        if (!filesContainer) {
            console.error('🚨 DIAGNOSTIC: Files container not found');
            return;
        }

        if (data.files.length === 0) {
            console.log('🔍 DIAGNOSTIC: No files to display');
            filesContainer.innerHTML = `
                <div class="alert alert-info text-center">
                    <p><i class="fas fa-info-circle mr-2"></i>No files found. Upload your first file above.</p>
                </div>
            `;
            return;
        }

        // DIAGNOSTIC: Check what HTML is being generated
        console.log('🔍 DIAGNOSTIC: Starting HTML generation...');
        
        // Generate table HTML (CURRENT INCOMPLETE VERSION)
        let tableHTML = `
            <div class="table-responsive">
                <table class="table" id="fileTable">
                    <thead>
                        <tr>
                            <th><i class="fas fa-file mr-1"></i> Filename</th>
                            <th><i class="fas fa-weight mr-1"></i> Size</th>
                            <th><i class="fas fa-link mr-1"></i> Public Link</th>
                            <th><i class="fas fa-lock-open mr-1"></i> Public Access</th>
                            <th><i class="fas fa-cogs mr-1"></i> Actions</th>
                        </tr>
                    </thead>
                    <tbody>
        `;

        data.files.forEach(file => {
            const fileId = file.id || '';
            const fileHash = file.file_hash || '';
            const isPublic = file.is_public || false;
            
            console.log(`🔍 DIAGNOSTIC: Processing file ${file.name}:`, {
                fileId, fileHash, isPublic,
                canShowToggle: !!(fileId && fileHash),
                canShowDelete: !!fileId
            });

            tableHTML += `
                <tr data-file-id="${fileId}" data-file-hash="${fileHash}">
                    <td><strong>${file.name}</strong></td>
                    <td>${formatFileSize(file.size)}</td>
                    <td>
                        ${fileHash ?
                `<a href="/d/${fileHash}" target="_blank" class="public-link">
                                <i class="fas fa-external-link-alt mr-1"></i>View
                            </a>` :
                '<span class="text-muted">N/A</span>'
            }
                    </td>
                    <td>
                        <label class="switch">
                            <input type="checkbox" class="public-toggle" ${isPublic ? 'checked' : ''}>
                            <span class="slider round"></span>
                        </label>
                    </td>
                    <td>
                        <a href="/d/${fileHash}" class="btn btn-primary btn-sm">
                            <i class="fas fa-download mr-1"></i>Download
                        </a>
                        <button class="btn btn-danger btn-sm delete-btn" data-file-id="${fileId}">
                            <i class="fas fa-trash-alt mr-1"></i>Delete
                        </button>
                    </td>
                </tr>
            `;
        });

        tableHTML += `</tbody></table></div>`;
        
        console.log('🔍 DIAGNOSTIC: Generated HTML length:', tableHTML.length);
        console.log('🔍 DIAGNOSTIC: HTML contains toggle switches:', tableHTML.includes('public-toggle'));
        console.log('🔍 DIAGNOSTIC: HTML contains delete buttons:', tableHTML.includes('delete-btn'));
        
        filesContainer.innerHTML = tableHTML;

        // DIAGNOSTIC: Check if elements were created
        const toggles = document.querySelectorAll('.public-toggle');
        const deleteButtons = document.querySelectorAll('.delete-btn');
        
        console.log('🔍 DIAGNOSTIC: Created elements:', {
            toggleCount: toggles.length,
            deleteButtonCount: deleteButtons.length,
            expectedCount: data.files.length
        });

        // Setup event handlers
        console.log('🔍 DIAGNOSTIC: Setting up event handlers...');
        setupFileActionEventHandlers();
        
        console.log('🔍 DIAGNOSTIC: File list refresh completed successfully');
        
    } catch (error) {
        console.error('🚨 DIAGNOSTIC: Error in loadFiles():', error);
        showStatus('Failed to refresh file list: ' + error.message, 'error');
    }
}

// Add diagnostic to setupFileActionEventHandlers function
const originalSetupFileActionEventHandlers = setupFileActionEventHandlers;
function setupFileActionEventHandlers() {
    console.log('🔍 DIAGNOSTIC: setupFileActionEventHandlers() called');
    
    const deleteButtons = document.querySelectorAll('.delete-btn');
    const toggleSwitches = document.querySelectorAll('.public-toggle');
    
    console.log('🔍 DIAGNOSTIC: Found elements for event handlers:', {
        deleteButtons: deleteButtons.length,
        toggleSwitches: toggleSwitches.length
    });
    
    // Call original function
    originalSetupFileActionEventHandlers();
    
    console.log('🔍 DIAGNOSTIC: Event handlers setup completed');
}

// ========== END DIAGNOSTIC LOGS ==========
'''
    
    return diagnostic_js

def add_backend_diagnostics():
    """
    Add diagnostic logs to the backend API endpoints
    """
    
    backend_diagnostics = '''
# Add these diagnostic logs to your Flask app.py

@app.route('/api/files')
@login_required
def get_files_api():
    """
    API endpoint to get user's files (for AJAX requests) - WITH DIAGNOSTICS
    """
    print("🔍 DIAGNOSTIC: /api/files endpoint called")
    try:
        user_database_id = uploader.get_user_database_id(current_user.id)
        print(f"🔍 DIAGNOSTIC: User database ID: {user_database_id}")
        
        if not user_database_id:
            print("🚨 DIAGNOSTIC: User database not found")
            return jsonify({'error': 'User database not found'}), 404
        
        files_response = uploader.get_files_from_user_database(user_database_id)
        files = files_response.get('results', [])
        
        print(f"🔍 DIAGNOSTIC: Raw files from database: {len(files)} files")
        
        # Format files for JSON response
        formatted_files = []
        for i, file_data in enumerate(files):
            file_props = file_data.get('properties', {})
            
            # Extract all properties
            file_id = file_data.get('id')
            name = file_props.get('filename', {}).get('title', [{}])[0].get('text', {}).get('content', 'Unknown')
            size = file_props.get('filesize', {}).get('number', 0)
            file_hash = file_props.get('filehash', {}).get('rich_text', [{}])[0].get('text', {}).get('content', '')
            is_public = file_props.get('is_public', {}).get('checkbox', False)
            
            print(f"🔍 DIAGNOSTIC: File {i+1} - {name}:")
            print(f"  - ID: {file_id}")
            print(f"  - Hash: {file_hash}")
            print(f"  - Is Public: {is_public}")
            print(f"  - Size: {size}")
            
            formatted_file = {
                'id': file_id,
                'name': name,
                'size': size,
                'file_hash': file_hash,
                'is_public': is_public
            }
            
            formatted_files.append(formatted_file)
        
        print(f"🔍 DIAGNOSTIC: Returning {len(formatted_files)} formatted files")
        print(f"🔍 DIAGNOSTIC: First file structure: {formatted_files[0] if formatted_files else 'None'}")
        
        return jsonify({'files': formatted_files})
        
    except Exception as e:
        print(f"🚨 DIAGNOSTIC: Error in /api/files: {e}")
        import traceback
        traceback.print_exc()
        return jsonify({'error': str(e)}), 500
'''
    
    return backend_diagnostics

def generate_complete_diagnostic_report():
    """
    Generate a complete diagnostic report
    """
    
    report = """
# 🚨 MISSING BUTTONS DIAGNOSTIC REPORT

## Issue Summary
After successful file upload, the toggle public access and delete buttons are missing from the file list, even though files appear correctly.

## Root Cause Analysis

### PRIMARY ISSUE: Incomplete Template Rendering in loadFiles()
The `loadFiles()` function in `static/streaming-upload.js` is only generating 4 table columns instead of 5:

**CURRENT (BROKEN) HTML Generation:**
```javascript
// Lines 441-482 in streaming-upload.js
tableHTML += `
    <th><i class="fas fa-file mr-1"></i> Filename</th>
    <th><i class="fas fa-weight mr-1"></i> Size</th>
    <th><i class="fas fa-link mr-1"></i> Public Link</th>
    <th><i class="fas fa-cogs mr-1"></i> Actions</th>  // MISSING PUBLIC ACCESS COLUMN
`;
```

**EXPECTED (WORKING) HTML Generation:**
```javascript
// Should match templates/home.html:35-41
tableHTML += `
    <th><i class="fas fa-file mr-1"></i> Filename</th>
    <th><i class="fas fa-weight mr-1"></i> Size</th>
    <th><i class="fas fa-link mr-1"></i> Public Link</th>
    <th><i class="fas fa-lock-open mr-1"></i> Public Access</th>  // MISSING!
    <th><i class="fas fa-cogs mr-1"></i> Actions</th>
`;
```

### SECONDARY ISSUE: Incomplete Action Buttons
The Actions column only includes Download button, missing Delete button:

**CURRENT (BROKEN):**
```javascript
<td>
    <a href="/d/${fileHash}" class="btn btn-primary btn-sm">
        <i class="fas fa-download mr-1"></i>Download
    </a>
    // MISSING DELETE BUTTON
</td>
```

**EXPECTED (WORKING):**
```javascript
<td class="action-buttons">
    <a href="/d/${fileHash}" class="btn btn-primary btn-sm">
        <i class="fas fa-download mr-1"></i>Download
    </a>
    <button class="btn btn-danger btn-sm delete-btn" data-file-id="${fileId}">
        <i class="fas fa-trash-alt mr-1"></i>Delete
    </button>
</td>
```

## Files Affected
1. `static/streaming-upload.js` - Lines 441-482 (loadFiles function)
2. `static/upload.js` - Lines 160-230 (loadFiles function) - Has correct implementation
3. `templates/home.html` - Lines 35-74 (server-side template) - Correct reference

## Fix Required
Update the `loadFiles()` function in `static/streaming-upload.js` to match the complete template structure from `static/upload.js` and `templates/home.html`.
"""
    
    return report

if __name__ == "__main__":
    print("Diagnostic Analysis Complete!")
    print("\n" + "="*60)
    print(generate_complete_diagnostic_report())
    print("="*60)
    
    print("\nFrontend Diagnostics:")
    print(add_frontend_diagnostics())
    
    print("\nBackend Diagnostics:")
    print(add_backend_diagnostics())