// Global state
let currentView = 'list';
let currentFiles = [];
let selectedFile = null;
let uploadFileId = null;
let allFiles = [];

// Initialize on load
document.addEventListener('DOMContentLoaded', () => {
    loadFiles();
    setupEventListeners();
});

// Setup event listeners
function setupEventListeners() {
    // Upload button
    document.getElementById('uploadBtn').addEventListener('click', openUploadModal);
    
    // File input
    const fileInput = document.getElementById('fileInput');
    fileInput.addEventListener('change', handleFileSelect);
    
    // Upload area drag & drop
    const uploadArea = document.getElementById('uploadArea');
    uploadArea.addEventListener('click', () => fileInput.click());
    uploadArea.addEventListener('dragover', handleDragOver);
    uploadArea.addEventListener('dragleave', handleDragLeave);
    uploadArea.addEventListener('drop', handleDrop);
    
    // View switcher
    document.querySelectorAll('.view-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const view = e.currentTarget.dataset.view;
            switchView(view);
        });
    });
    
    // Search
    document.getElementById('searchInput').addEventListener('input', handleSearch);
    
    // Sort
    document.getElementById('sortSelect').addEventListener('change', handleSort);
}

// 🔧 FIX: Função para obter ícone baseado no tipo de arquivo
function getFileIcon(filename) {
    const ext = filename.split('.').pop().toLowerCase();
    
    const iconMap = {
        'pdf': 'fa-file-pdf',
        'doc': 'fa-file-word',
        'docx': 'fa-file-word',
        'xls': 'fa-file-excel',
        'xlsx': 'fa-file-excel',
        'ppt': 'fa-file-powerpoint',
        'pptx': 'fa-file-powerpoint',
        'jpg': 'fa-file-image',
        'jpeg': 'fa-file-image',
        'png': 'fa-file-image',
        'gif': 'fa-file-image',
        'svg': 'fa-file-image',
        'zip': 'fa-file-archive',
        'rar': 'fa-file-archive',
        '7z': 'fa-file-archive',
        'txt': 'fa-file-lines',
        'md': 'fa-file-lines',
        'py': 'fa-file-code',
        'js': 'fa-file-code',
        'html': 'fa-file-code',
        'css': 'fa-file-code',
        'json': 'fa-file-code',
        'mp3': 'fa-file-audio',
        'wav': 'fa-file-audio',
        'mp4': 'fa-file-video',
        'avi': 'fa-file-video',
    };
    
    return iconMap[ext] || 'fa-file';
}

// Load files from server
async function loadFiles() {
    try {
        const response = await fetch('/api/files');

        if (!response.ok) {
            const text = await response.text();
            throw new Error(`HTTP ${response.status}: ${text}`);
        }

        const data = await response.json();

        if (!data.success || !Array.isArray(data.files)) {
            throw new Error(data.error || 'Invalid server response');
        }

        // 🔧 FIX: Adiciona ícone para cada arquivo
        allFiles = data.files.map(file => ({
            ...file,
            icon: getFileIcon(file.name)
        }));

        currentFiles = [...allFiles];

        renderFiles();
        updateStorageInfo();

    } catch (error) {
        console.error('Error loading files:', error);
        showNotification('Error loading files: ' + error.message, 'error');

        allFiles = [];
        currentFiles = [];
        renderFiles();
        updateStorageInfo();
    }
}

// Render files in current view
function renderFiles() {
    const emptyState = document.getElementById('emptyState');
    const filesContainer = document.getElementById('filesContainer');
    
    if (currentFiles.length === 0) {
        emptyState.style.display = 'block';
        filesContainer.style.display = 'none';
        return;
    }
    
    emptyState.style.display = 'none';
    filesContainer.style.display = 'block';
    
    if (currentView === 'list') {
        renderListView();
    } else {
        renderGridView();
    }
}

// Render list view
function renderListView() {
    const fileList = document.getElementById('fileList');
    const existingItems = fileList.querySelectorAll('.file-item');
    existingItems.forEach(item => item.remove());
    
    currentFiles.forEach(file => {
        const row = document.createElement('div');
        row.className = 'file-item';
        
        // 🔧 FIX: Extrai apenas a classe do ícone (remove 'fa-')
        const iconType = file.icon.replace('fa-file-', '') || 'default';
        
        row.innerHTML = `
            <div class="file-item-name">
                <div class="file-icon-container">
                    <i class="fas ${file.icon} file-icon-${iconType}"></i>
                </div>
                <span>${escapeHtml(file.name)}</span>
            </div>
            <div>${file.size}</div>
            <div>${file.chunks}</div>
            <div>${file.uploaded}</div>
            <div class="file-actions">
                <button class="action-btn primary" onclick="downloadFile('${file.id}')">
                    <i class="fas fa-download"></i> Download
                </button>
                <button class="action-btn danger" onclick="deleteFile('${file.id}')">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        `;
        fileList.appendChild(row);
    });
}

// Render grid view
function renderGridView() {
    const fileGrid = document.getElementById('fileGrid');
    fileGrid.innerHTML = '';
    
    currentFiles.forEach(file => {
        const card = document.createElement('div');
        card.className = 'file-card';
        card.innerHTML = `
            <div class="file-card-icon">
                <i class="fas ${file.icon}"></i>
            </div>
            <div class="file-card-name" title="${escapeHtml(file.name)}">${escapeHtml(file.name)}</div>
            <div class="file-card-size">${file.size}</div>
            <div class="file-actions" style="margin-top: 12px;">
                <button class="action-btn primary" onclick="downloadFile('${file.id}')">
                    <i class="fas fa-download"></i>
                </button>
                <button class="action-btn danger" onclick="deleteFile('${file.id}')">
                    <i class="fas fa-trash"></i>
                </button>
            </div>
        `;
        fileGrid.appendChild(card);
    });
}

// Switch view
function switchView(view) {
    currentView = view;
    
    // Update buttons
    document.querySelectorAll('.view-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.view === view);
    });
    
    // Toggle views
    const fileList = document.getElementById('fileList');
    const fileGrid = document.getElementById('fileGrid');
    
    if (view === 'list') {
        fileList.style.display = 'block';
        fileGrid.style.display = 'none';
    } else {
        fileList.style.display = 'none';
        fileGrid.style.display = 'grid';
    }
    
    renderFiles();
}

// Upload Modal
function openUploadModal() {
    document.getElementById('uploadModal').classList.add('show');
    resetUploadModal();
}

function closeUploadModal() {
    document.getElementById('uploadModal').classList.remove('show');
    resetUploadModal();
}

function resetUploadModal() {
    document.getElementById('fileInput').value = '';
    document.getElementById('uploadArea').style.display = 'block';
    document.getElementById('fileInfo').style.display = 'none';
    document.getElementById('progressContainer').style.display = 'none';
    document.getElementById('uploadStartBtn').disabled = true;
    selectedFile = null;
    
    // Limpa intervalos de atualização de tempo
    if (localUpdateInterval) {
        clearInterval(localUpdateInterval);
        localUpdateInterval = null;
    }
    lastServerElapsed = 0;
    lastServerRemaining = 0;
    localStartTime = null;
}

// File selection
function handleFileSelect(e) {
    const file = e.target.files[0];
    if (file) {
        processFile(file);
    }
}

function handleDragOver(e) {
    e.preventDefault();
    e.currentTarget.classList.add('drag-over');
}

function handleDragLeave(e) {
    e.currentTarget.classList.remove('drag-over');
}

function handleDrop(e) {
    e.preventDefault();
    e.currentTarget.classList.remove('drag-over');
    
    const file = e.dataTransfer.files[0];
    if (file) {
        processFile(file);
    }
}

async function processFile(file) {
    selectedFile = file;
    
    // Show file info
    document.getElementById('uploadArea').style.display = 'none';
    document.getElementById('fileInfo').style.display = 'block';
    
    // Update file details
    document.getElementById('fileIcon').className = `fas ${getFileIcon(file.name)}`;
    document.getElementById('fileName').textContent = file.name;
    document.getElementById('fileSize').textContent = formatFileSize(file.size);
    
    // Get estimate
    try {
        const formData = new FormData();
        formData.append('file', file);
        
        const response = await fetch('/api/estimate', {
            method: 'POST',
            body: formData
        });
        
        if (response.ok) {
            const data = await response.json();
            if (data.success) {
                document.getElementById('estChunks').textContent = data.estimate.chunks;
                document.getElementById('estUploadTime').textContent = data.estimate.upload_time;
                document.getElementById('estDownloadTime').textContent = data.estimate.download_time;
                document.getElementById('estimateInfo').style.display = 'block';
            }
        }
    } catch (error) {
        console.error('Estimate error:', error);
    }
    
    // Enable upload button
    document.getElementById('uploadStartBtn').disabled = false;
}

// Start upload
async function startUpload() {
    if (!selectedFile) return;
    
    document.getElementById('uploadStartBtn').disabled = true;
    document.getElementById('progressContainer').style.display = 'block';
    
    try {
        const formData = new FormData();
        formData.append('file', selectedFile);
        
        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });
        
        if (!response.ok) {
            const text = await response.text();
            throw new Error(`HTTP ${response.status}: ${text}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            uploadFileId = data.file_id;
            monitorUploadProgress(uploadFileId);
        } else {
            showNotification('Upload failed: ' + data.error, 'error');
            closeUploadModal();
        }
    } catch (error) {
        showNotification('Upload error: ' + error.message, 'error');
        closeUploadModal();
    }
}

// Monitor upload progress
let localStartTime = null;
let lastServerElapsed = 0;
let lastServerRemaining = 0;
let localUpdateInterval = null;

async function monitorUploadProgress(fileId) {
    localStartTime = Date.now() / 1000; // em segundos
    
    // Atualiza o tempo localmente a cada 100ms
    localUpdateInterval = setInterval(() => {
        if (lastServerElapsed > 0 || lastServerRemaining > 0) {
            const currentLocalElapsed = (Date.now() / 1000) - localStartTime + lastServerElapsed;
            const currentLocalRemaining = Math.max(0, lastServerRemaining - ((Date.now() / 1000) - localStartTime));
            
            const elapsedEl = document.getElementById('elapsedTime');
            const remainingEl = document.getElementById('remainingTime');
            
            if (elapsedEl) {
                elapsedEl.textContent = `Elapsed: ${formatTime(currentLocalElapsed)}`;
            }
            if (remainingEl) {
                remainingEl.textContent = `Remaining: ${formatTime(currentLocalRemaining)}`;
            }
        }
    }, 100); // Atualiza a cada 100ms para suavidade
    
    const interval = setInterval(async () => {
        try {
            const response = await fetch(`/api/upload/progress/${fileId}`);
            
            if (!response.ok) {
                const text = await response.text();
                throw new Error(`HTTP ${response.status}: ${text}`);
            }
            
            const data = await response.json();
            
            if (data.success) {
                const progress = data.progress;
                
                // Tratamento seguro de valores undefined
                const percent = progress.progress || 0;
                const message = progress.message || 'Processing...';
                const status = progress.status || 'uploading';
                const elapsedTime = progress.elapsed_time || 0;
                const estimatedRemaining = progress.estimated_remaining || 0;
                
                // Atualiza valores do servidor
                lastServerElapsed = elapsedTime;
                lastServerRemaining = estimatedRemaining;
                localStartTime = Date.now() / 1000; // reseta o tempo local
                
                // Update progress bar
                document.getElementById('progressFill').style.width = percent + '%';
                document.getElementById('progressPercent').textContent = percent + '%';
                document.getElementById('progressMessage').textContent = message;
                
                // Check if complete
                if (status === 'completed') {
                    clearInterval(interval);
                    if (localUpdateInterval) {
                        clearInterval(localUpdateInterval);
                        localUpdateInterval = null;
                    }
                    setTimeout(() => {
                        closeUploadModal();
                        loadFiles();
                        showNotification('File uploaded successfully!', 'success');
                    }, 1000);
                } else if (status === 'error') {
                    clearInterval(interval);
                    if (localUpdateInterval) {
                        clearInterval(localUpdateInterval);
                        localUpdateInterval = null;
                    }
                    showNotification('Upload failed: ' + message, 'error');
                    closeUploadModal();
                }
            }
        } catch (error) {
            clearInterval(interval);
            if (localUpdateInterval) {
                clearInterval(localUpdateInterval);
                localUpdateInterval = null;
            }
            console.error('Upload monitoring error:', error);
            showNotification('Error monitoring upload: ' + error.message, 'error');
            closeUploadModal();
        }
    }, 1000); // Busca do servidor a cada 1 segundo
}

// Download file
async function downloadFile(fileId) {
    showNotification('Starting download...', 'info');
    
    try {
        window.location.href = `/api/download/${fileId}`;
        setTimeout(() => {
            showNotification('Download started!', 'success');
        }, 1000);
    } catch (error) {
        showNotification('Download failed: ' + error.message, 'error');
    }
}

// Delete file
async function deleteFile(fileId) {
    if (!confirm('Are you sure you want to delete this file?')) {
        return;
    }
    
    try {
        const response = await fetch(`/api/delete/${fileId}`, {
            method: 'DELETE'
        });
        
        if (!response.ok) {
            const text = await response.text();
            throw new Error(`HTTP ${response.status}: ${text}`);
        }
        
        const data = await response.json();
        
        if (data.success) {
            loadFiles();
            showNotification('File deleted successfully!', 'success');
        } else {
            showNotification('Delete failed: ' + data.error, 'error');
        }
    } catch (error) {
        showNotification('Delete error: ' + error.message, 'error');
    }
}

// Search
function handleSearch(e) {
    const query = e.target.value.trim().toLowerCase();

    if (query === '') {
        currentFiles = [...allFiles];
    } else {
        currentFiles = allFiles.filter(file =>
            file.name.toLowerCase().includes(query)
        );
    }

    renderFiles();
    updateStorageInfo();
}

// Sort
function handleSort(e) {
    const sortBy = e.target.value;
    
    if (sortBy === 'name') {
        currentFiles.sort((a, b) => a.name.localeCompare(b.name));
    } else if (sortBy === 'size') {
        currentFiles.sort((a, b) => b.size_bytes - a.size_bytes);
    } else if (sortBy === 'date') {
        currentFiles.sort((a, b) => new Date(b.uploaded) - new Date(a.uploaded));
    }
    
    renderFiles();
}

// Update storage info
function updateStorageInfo() {
    const totalSize = currentFiles.reduce((sum, file) => sum + (file.size_bytes || 0), 0);
    const percent = Math.min((totalSize / (1024 * 1024 * 100)) * 100, 100); // Max 100MB como exemplo
    
    document.getElementById('storageUsed').textContent = formatFileSize(totalSize);
    document.querySelector('.storage-used').style.width = percent + '%';
}

// Utility functions
function formatFileSize(bytes) {
    if (bytes === 0) return '0 B';
    const k = 1024;
    const sizes = ['B', 'KB', 'MB', 'GB', 'TB'];
    const i = Math.floor(Math.log(bytes) / Math.log(k));
    return Math.round((bytes / Math.pow(k, i)) * 100) / 100 + ' ' + sizes[i];
}

function formatTime(seconds) {
    const hours = Math.floor(seconds / 3600);
    const minutes = Math.floor((seconds % 3600) / 60);
    const secs = Math.floor(seconds % 60);
    return `${String(hours).padStart(2, '0')}:${String(minutes).padStart(2, '0')}:${String(secs).padStart(2, '0')}`;
}

function escapeHtml(text) {
    const div = document.createElement('div');
    div.textContent = text;
    return div.innerHTML;
}

function showNotification(message, type = 'success') {
    const notification = document.getElementById('notification');
    const icon = notification.querySelector('i');
    
    // Set icon based on type
    if (type === 'success') {
        icon.className = 'fas fa-check-circle';
    } else if (type === 'error') {
        icon.className = 'fas fa-exclamation-circle';
    } else {
        icon.className = 'fas fa-info-circle';
    }
    
    document.getElementById('notificationText').textContent = message;
    notification.classList.add('show');
    
    setTimeout(() => {
        notification.classList.remove('show');
    }, 3000);
}