// Global state
let currentView = 'list';
let currentFiles = [];
let selectedFile = null;
let uploadFileId = null;
let allFiles = [];
let currentFilter = 'all';
let sortDescending = true;
let serverConfig = {};

// Initialize on load
document.addEventListener('DOMContentLoaded', async () => {
    await loadConfig();
    loadFiles();
    setupEventListeners();
});

// Load server capabilities (keys available, limits, strict default)
async function loadConfig() {
    try {
        const response = await fetch('/api/config');
        const data = await response.json();

        if (!data.success) return;

        serverConfig = data.config;

        document.getElementById('protocolBadge').textContent =
            'BSP ' + serverConfig.protocol_version;
        document.getElementById('maxSizeLabel').textContent =
            serverConfig.max_file_size_label;
        document.getElementById('strictToggle').checked =
            serverConfig.default_strict;

        applyKeyAvailability();

    } catch (error) {
        console.error('Could not load server config:', error);
    }
}

// The sign checkbox is only meaningful when the server holds a private key
function applyKeyAvailability() {
    const checkbox = document.getElementById('signCheckbox');
    const hint = document.getElementById('signHint');
    const row = document.getElementById('signRow');

    if (serverConfig.has_private_key) {
        checkbox.disabled = false;
        checkbox.checked = true;
        row.classList.remove('disabled');
        hint.textContent =
            `Proves authenticity on download — BSP v4 (${serverConfig.private_key_path})`;
    } else {
        checkbox.disabled = true;
        checkbox.checked = false;
        row.classList.add('disabled');
        hint.textContent =
            `No private key at ${serverConfig.private_key_path} — see Help to generate one`;
    }
}

// Setup event listeners
function setupEventListeners() {
    // Upload button
    document.getElementById('uploadBtn').addEventListener('click', openUploadModal);

    // Import metadata
    document.getElementById('importBtn').addEventListener('click', openImportModal);

    // File input
    const fileInput = document.getElementById('fileInput');
    fileInput.addEventListener('change', handleFileSelect);

    // Upload area drag & drop
    const uploadArea = document.getElementById('uploadArea');
    uploadArea.addEventListener('click', () => fileInput.click());
    uploadArea.addEventListener('dragover', handleDragOver);
    uploadArea.addEventListener('dragleave', handleDragLeave);
    uploadArea.addEventListener('drop', handleDrop);

    // Import area drag & drop
    const importInput = document.getElementById('importInput');
    const importArea = document.getElementById('importArea');
    importInput.addEventListener('change', (e) => {
        if (e.target.files[0]) importMetadata(e.target.files[0]);
    });
    importArea.addEventListener('click', () => importInput.click());
    importArea.addEventListener('dragover', handleDragOver);
    importArea.addEventListener('dragleave', handleDragLeave);
    importArea.addEventListener('drop', (e) => {
        e.preventDefault();
        e.currentTarget.classList.remove('drag-over');
        if (e.dataTransfer.files[0]) importMetadata(e.dataTransfer.files[0]);
    });

    // View switcher
    document.querySelectorAll('.view-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
            const view = e.currentTarget.dataset.view;
            switchView(view);
        });
    });

    // Sidebar filters
    document.querySelectorAll('.nav-item').forEach(item => {
        item.addEventListener('click', (e) => {
            e.preventDefault();
            applyFilter(e.currentTarget.dataset.filter);
        });
    });

    // Search
    document.getElementById('searchInput').addEventListener('input', applyListState);

    // Sort
    document.getElementById('sortSelect').addEventListener('change', applyListState);
    document.getElementById('sortDirBtn').addEventListener('click', toggleSortDirection);

    // Strict mode is remembered for the session
    document.getElementById('strictToggle').addEventListener('change', (e) => {
        showNotification(
            e.target.checked
                ? 'Strict mode on — restores abort on any integrity failure'
                : 'Strict mode off — integrity failures are reported as warnings',
            'info'
        );
    });

    // Settings / Help
    document.getElementById('settingsBtn').addEventListener('click', openSettingsModal);
    document.getElementById('helpBtn').addEventListener('click', openHelpModal);

    // Close any modal with Escape
    document.addEventListener('keydown', (e) => {
        if (e.key === 'Escape') {
            document.querySelectorAll('.modal.show').forEach(m => m.classList.remove('show'));
        }
    });
}

function isStrict() {
    return document.getElementById('strictToggle').checked;
}

// Icon for a file type
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

function iconColorClass(icon) {
    // 'fa-file-pdf' -> 'file-icon-pdf'; plain 'fa-file' -> 'file-icon-default'
    return icon === 'fa-file'
        ? 'file-icon-default'
        : 'file-icon-' + icon.replace('fa-file-', '');
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

        allFiles = data.files.map(file => ({
            ...file,
            icon: getFileIcon(file.name)
        }));

        applyListState();

    } catch (error) {
        console.error('Error loading files:', error);
        showNotification('Error loading files: ' + error.message, 'error');

        allFiles = [];
        applyListState();
    }
}

// Filter + search + sort, then render
function applyListState() {
    const query = document.getElementById('searchInput').value.trim().toLowerCase();
    const sortBy = document.getElementById('sortSelect').value;

    currentFiles = allFiles.filter(file => {
        if (currentFilter === 'signed' && !file.signed) return false;
        if (currentFilter === 'unsigned' && file.signed) return false;
        if (currentFilter === 'incomplete' && !file.missing_coordinates) return false;
        if (query && !file.name.toLowerCase().includes(query)) return false;
        return true;
    });

    const direction = sortDescending ? 1 : -1;

    currentFiles.sort((a, b) => {
        if (sortBy === 'name') return a.name.localeCompare(b.name) * -direction;
        if (sortBy === 'size') return (b.size_bytes - a.size_bytes) * direction;
        if (sortBy === 'chunks') return (b.chunks - a.chunks) * direction;
        return (b.uploaded_ts - a.uploaded_ts) * direction;
    });

    renderFiles();
    updateStorageInfo();
    updateCounts();
}

function applyFilter(filter) {
    currentFilter = filter;

    document.querySelectorAll('.nav-item').forEach(item => {
        item.classList.toggle('active', item.dataset.filter === filter);
    });

    applyListState();
}

function toggleSortDirection() {
    sortDescending = !sortDescending;

    const icon = document.querySelector('#sortDirBtn i');
    icon.className = sortDescending
        ? 'fas fa-arrow-down-wide-short'
        : 'fas fa-arrow-up-short-wide';

    applyListState();
}

function updateCounts() {
    document.getElementById('countAll').textContent = allFiles.length;
    document.getElementById('countSigned').textContent =
        allFiles.filter(f => f.signed).length;
    document.getElementById('countUnsigned').textContent =
        allFiles.filter(f => !f.signed).length;
    document.getElementById('countIncomplete').textContent =
        allFiles.filter(f => f.missing_coordinates).length;
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

// Badges shown next to a file name
function fileBadges(file) {
    let badges = '';

    if (file.signed) {
        if (file.signature_valid === false) {
            // Signed, but the signature no longer verifies with the current
            // public key — typically after the key pair was regenerated.
            badges += `<span class="badge invalid" title="Signed, but the signature does NOT verify with the current public key. The key was likely regenerated — re-sign this file to restore a valid signature.">
                <i class="fas fa-triangle-exclamation"></i> signature invalid</span>`;
        } else if (file.signature_valid === true) {
            badges += `<span class="badge signed" title="Signature verified with the current public key (BSP v4)">
                <i class="fas fa-file-signature"></i> signed</span>`;
        } else {
            // null — signed but there is no public key on the server to check it
            badges += `<span class="badge muted" title="Signed, but no public key on the server to verify the signature">
                <i class="fas fa-file-signature"></i> signed (unverified)</span>`;
        }
    }
    if (file.missing_coordinates) {
        badges += `<span class="badge warn" title="${file.missing_coordinates} chunk(s) have no Babel coordinates — this file cannot be restored">
            <i class="fas fa-triangle-exclamation"></i> ${file.missing_coordinates} missing</span>`;
    }

    return badges;
}

// Action buttons shared by both views
function fileActions(file, compact) {
    const label = (text) => compact ? '' : ` ${text}`;

    return `
        <button class="action-btn primary" onclick="downloadFile('${file.id}')" title="Restore from Babel">
            <i class="fas fa-download"></i>${label('Download')}
        </button>
        <button class="action-btn" onclick="showFileInfo('${file.id}')" title="Chunk coordinates and hashes (info)">
            <i class="fas fa-circle-info"></i>
        </button>
        <button class="action-btn" onclick="verifyFile('${file.id}')" title="Offline metadata verification (verify-metadata)">
            <i class="fas fa-shield-halved"></i>
        </button>
        <button class="action-btn" onclick="exportMetadata('${file.id}')" title="Export the .json.gz metadata — back this up!">
            <i class="fas fa-file-export"></i>
        </button>
        <button class="action-btn danger" onclick="deleteFile('${file.id}')" title="Delete metadata">
            <i class="fas fa-trash"></i>
        </button>
    `;
}

// Render list view
function renderListView() {
    const fileList = document.getElementById('fileList');
    const existingItems = fileList.querySelectorAll('.file-item');
    existingItems.forEach(item => item.remove());

    currentFiles.forEach(file => {
        const row = document.createElement('div');
        row.className = 'file-item';

        row.innerHTML = `
            <div class="file-item-name">
                <div class="file-icon-container">
                    <i class="fas ${file.icon} ${iconColorClass(file.icon)}"></i>
                </div>
                <span class="file-item-label" title="${escapeHtml(file.name)}">${escapeHtml(file.name)}</span>
                ${fileBadges(file)}
            </div>
            <div>${file.size}</div>
            <div>${file.chunks}</div>
            <div>${file.uploaded}</div>
            <div class="file-actions">${fileActions(file, true)}</div>
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
                <i class="fas ${file.icon} ${iconColorClass(file.icon)}"></i>
            </div>
            <div class="file-card-name" title="${escapeHtml(file.name)}">${escapeHtml(file.name)}</div>
            <div class="file-card-size">${file.size} · ${file.chunks} chunks</div>
            <div class="badge-row">${fileBadges(file)}</div>
            <div class="file-actions" style="margin-top: 12px;">${fileActions(file, true)}</div>
        `;
        fileGrid.appendChild(card);
    });
}

// Switch view
function switchView(view) {
    currentView = view;

    document.querySelectorAll('.view-btn').forEach(btn => {
        btn.classList.toggle('active', btn.dataset.view === view);
    });

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

// ============================================================
// UPLOAD
// ============================================================

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
    document.getElementById('estimateInfo').style.display = 'none';
    document.getElementById('progressContainer').style.display = 'none';
    document.getElementById('uploadStartBtn').disabled = true;
    document.getElementById('chunkCounter').textContent = '';
    selectedFile = null;

    applyKeyAvailability();
    stopUploadTimers();
}

function stopUploadTimers() {
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

    document.getElementById('uploadArea').style.display = 'none';
    document.getElementById('fileInfo').style.display = 'block';

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
                document.getElementById('estCompressed').textContent =
                    `${data.estimate.compressed_size} (${data.estimate.compression_ratio}% of original)`;
                document.getElementById('estimateInfo').style.display = 'block';
            }
        }
    } catch (error) {
        console.error('Estimate error:', error);
    }

    document.getElementById('uploadStartBtn').disabled = false;
}

// Start upload
async function startUpload() {
    if (!selectedFile) return;

    document.getElementById('uploadStartBtn').disabled = true;
    document.getElementById('progressContainer').style.display = 'block';

    const sign = document.getElementById('signCheckbox').checked;

    try {
        const formData = new FormData();
        formData.append('file', selectedFile);
        formData.append('sign', sign ? '1' : '0');

        const response = await fetch('/api/upload', {
            method: 'POST',
            body: formData
        });

        const data = await response.json().catch(() => null);

        if (!response.ok || !data || !data.success) {
            throw new Error((data && data.error) || `HTTP ${response.status}`);
        }

        uploadFileId = data.file_id;
        monitorUploadProgress(uploadFileId);

    } catch (error) {
        showNotification('Upload error: ' + error.message, 'error');
        document.getElementById('uploadStartBtn').disabled = false;
        document.getElementById('progressContainer').style.display = 'none';
    }
}

// Monitor upload progress
let localStartTime = null;
let lastServerElapsed = 0;
let lastServerRemaining = 0;
let localUpdateInterval = null;

async function monitorUploadProgress(fileId) {
    localStartTime = Date.now() / 1000;

    // Smooth the clocks between server polls
    localUpdateInterval = setInterval(() => {
        if (lastServerElapsed > 0 || lastServerRemaining > 0) {
            const drift = (Date.now() / 1000) - localStartTime;

            setText('elapsedTime', `Elapsed: ${formatTime(lastServerElapsed + drift)}`);
            setText('remainingTime',
                `Remaining: ${formatTime(Math.max(0, lastServerRemaining - drift))}`);
        }
    }, 100);

    const interval = setInterval(async () => {
        try {
            const response = await fetch(`/api/upload/progress/${fileId}`);

            if (!response.ok) {
                const text = await response.text();
                throw new Error(`HTTP ${response.status}: ${text}`);
            }

            const data = await response.json();

            if (!data.success) return;

            const progress = data.progress;
            const percent = progress.progress || 0;
            const message = progress.message || 'Processing...';
            const status = progress.status || 'running';

            lastServerElapsed = progress.elapsed_time || 0;
            lastServerRemaining = progress.estimated_remaining || 0;
            localStartTime = Date.now() / 1000;

            document.getElementById('progressFill').style.width = percent + '%';
            setText('progressPercent', percent + '%');
            setText('progressMessage', message);

            if (progress.total_chunks) {
                setText('chunkCounter',
                    `Chunk ${progress.current_chunk || 0}/${progress.total_chunks}`);
            }

            if (status === 'completed') {
                clearInterval(interval);
                stopUploadTimers();
                setTimeout(() => {
                    closeUploadModal();
                    loadFiles();
                    showNotification(
                        progress.signed
                            ? 'File uploaded and metadata signed!'
                            : 'File uploaded successfully!',
                        'success'
                    );
                }, 1000);
            } else if (status === 'error') {
                clearInterval(interval);
                stopUploadTimers();
                showNotification('Upload failed: ' + message, 'error');
                document.getElementById('uploadStartBtn').disabled = false;
            }

        } catch (error) {
            clearInterval(interval);
            stopUploadTimers();
            console.error('Upload monitoring error:', error);
            showNotification('Error monitoring upload: ' + error.message, 'error');
        }
    }, 1000);
}

// ============================================================
// DOWNLOAD
// ============================================================

let downloadPollInterval = null;

function closeDownloadModal() {
    document.getElementById('downloadModal').classList.remove('show');
    if (downloadPollInterval) {
        clearInterval(downloadPollInterval);
        downloadPollInterval = null;
    }
}

async function downloadFile(fileId) {
    const file = allFiles.find(f => f.id === fileId);
    const strict = isStrict();

    try {
        const response = await fetch(`/api/download/${fileId}/start`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ strict })
        });

        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        document.getElementById('downloadModal').classList.add('show');
        setText('downloadName', data.filename);
        setText('downloadMeta',
            `${file ? file.size + ' · ' : ''}${data.total_chunks} chunks to retrieve`);

        let badges = data.strict
            ? '<span class="badge strict"><i class="fas fa-shield-halved"></i> strict mode</span>'
            : '<span class="badge warn"><i class="fas fa-triangle-exclamation"></i> strict mode off</span>';

        if (data.verifying_signature) {
            badges += '<span class="badge signed"><i class="fas fa-file-signature"></i> verifying signature</span>';
        }

        document.getElementById('downloadBadges').innerHTML = badges;
        const fill = document.getElementById('downloadFill');
        fill.classList.remove('failed');  // reset from a previous failed run
        fill.style.width = '0%';
        setText('downloadPercent', '0%');
        setText('downloadMessage', 'Starting...');

        monitorDownload(data.job_id);

    } catch (error) {
        showNotification('Download failed: ' + error.message, 'error');
    }
}

function monitorDownload(jobId) {
    if (downloadPollInterval) clearInterval(downloadPollInterval);

    downloadPollInterval = setInterval(async () => {
        try {
            const response = await fetch(`/api/download/job/${jobId}`);
            const data = await response.json();

            if (!response.ok || !data.success) {
                throw new Error(data.error || `HTTP ${response.status}`);
            }

            const job = data.progress;
            const percent = job.progress || 0;

            document.getElementById('downloadFill').style.width = percent + '%';
            setText('downloadPercent', percent + '%');
            setText('downloadMessage', job.message || 'Working...');
            setText('downloadElapsed', `Elapsed: ${formatTime(job.elapsed_time || 0)}`);
            setText('downloadRemaining',
                `Remaining: ${formatTime(job.estimated_remaining || 0)}`);

            if (job.total_chunks) {
                setText('downloadChunks',
                    `Chunk ${job.current_chunk || 0}/${job.total_chunks}`);
            }

            if (job.status === 'completed') {
                clearInterval(downloadPollInterval);
                downloadPollInterval = null;

                // The bytes are held server-side until fetched exactly once
                window.location.href = `/api/download/job/${jobId}/file`;

                setTimeout(() => {
                    closeDownloadModal();
                    showNotification('File restored and verified!', 'success');
                }, 1200);

            } else if (job.status === 'error') {
                clearInterval(downloadPollInterval);
                downloadPollInterval = null;
                setText('downloadMessage', job.message || 'Download failed');
                document.getElementById('downloadFill').classList.add('failed');
                showNotification(job.error || 'Download failed', 'error');
            }

        } catch (error) {
            clearInterval(downloadPollInterval);
            downloadPollInterval = null;
            showNotification('Error monitoring download: ' + error.message, 'error');
        }
    }, 1000);
}

// ============================================================
// INFO  (CLI: info)
// ============================================================

function closeInfoModal() {
    document.getElementById('infoModal').classList.remove('show');
}

async function showFileInfo(fileId) {
    const container = document.getElementById('fileInfoContent');
    container.innerHTML = '<p class="modal-note">Loading...</p>';
    document.getElementById('infoModal').classList.add('show');

    try {
        const response = await fetch(`/api/files/${fileId}/info`);
        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        const info = data.info;
        const canVerifyChunk = !!info.merkle_root;

        const rows = info.chunks.map(chunk => {
            const c = chunk.coords;
            const coordText = c
                ? `${c.hex.substring(0, 8)}…/${c.wall}/${c.shelf}/${c.volume}/${c.page}`
                : '<span class="text-warn">NOT UPLOADED</span>';
            const coordTitle = c ? `hex ${c.hex}` : 'No coordinates recorded';

            const verifyCell = (canVerifyChunk && c)
                ? `<button class="action-btn chunk-verify" data-index="${chunk.index}"
                        onclick="verifyChunk('${info.id}', ${chunk.index}, this)"
                        title="Retrieve only this chunk and check its Merkle proof">
                        <i class="fas fa-shield-halved"></i>
                   </button>`
                : '<span class="text-muted">—</span>';

            return `<tr data-chunk-row="${chunk.index}">
                <td>${String(chunk.index).padStart(3, '0')}</td>
                <td>${chunk.size.toLocaleString()}</td>
                <td class="mono" title="${chunk.hash}">${chunk.hash.substring(0, 12)}…</td>
                <td class="mono" title="${escapeHtml(coordTitle)}">${coordText}</td>
                <td class="chunk-verify-cell">${verifyCell}</td>
            </tr>`;
        }).join('');

        const merkleRow = info.merkle_root
            ? `<tr><th>Merkle root</th><td class="mono break">${info.merkle_root}
                 <small class="text-muted">(height ${info.merkle_height})</small></td></tr>`
            : `<tr><th>Merkle root</th><td><span class="badge warn">none (pre-BSP v6)</span></td></tr>`;

        container.innerHTML = `
            <table class="kv-table">
                <tr><th>Filename</th><td>${escapeHtml(info.filename)}</td></tr>
                <tr><th>Size</th><td>${info.original_size.toLocaleString()} bytes</td></tr>
                <tr><th>File SHA-256</th><td class="mono break">${info.file_hash}</td></tr>
                <tr><th>Chunks</th><td>${info.uploaded_chunks} / ${info.chunk_count} with coordinates</td></tr>
                <tr><th>Protocol</th><td>BSP ${info.protocol_version}</td></tr>
                <tr><th>Signature</th><td>${!info.signed
                    ? '<span class="badge muted">not signed</span>'
                    : info.signature_valid === false
                        ? '<span class="badge invalid"><i class="fas fa-triangle-exclamation"></i> signed — does not verify with the current key</span>'
                        : info.signature_valid === true
                            ? '<span class="badge signed"><i class="fas fa-file-signature"></i> signed (verified, RSA-PSS)</span>'
                            : '<span class="badge muted"><i class="fas fa-file-signature"></i> signed (no public key to verify)</span>'
                }</td></tr>
                ${merkleRow}
                <tr><th>Metadata</th><td>${formatFileSize(info.metadata_size)} · ${info.uploaded}</td></tr>
            </table>

            ${resignBlock(info)}

            <h4 class="section-title">Chunks</h4>
            ${canVerifyChunk ? `<p class="modal-note">
                The shield verifies a single chunk against the Merkle root by fetching
                only that chunk from Babel — no full download (BSP v6).
            </p>` : ''}
            <div class="table-scroll">
                <table class="chunk-table">
                    <thead>
                        <tr><th>#</th><th>Bytes</th><th>SHA-256</th><th>hex/wall/shelf/volume/page</th><th>Verify</th></tr>
                    </thead>
                    <tbody>${rows}</tbody>
                </table>
            </div>
        `;

    } catch (error) {
        container.innerHTML =
            `<p class="verify-fail">Could not load info: ${escapeHtml(error.message)}</p>`;
    }
}

// Offer re-signing when the server holds a private key and the file is
// either unsigned or its signature no longer verifies (e.g. after the key
// pair was regenerated).
function resignBlock(info) {
    const invalid = info.signature_valid === false;
    const canResign = serverConfig.has_private_key && (invalid || !info.signed);

    if (!canResign) return '';

    return `
        <div class="resign-block ${invalid ? 'invalid' : ''}">
            <button class="secondary-btn" id="resignBtn" onclick="resignFile('${info.id}')">
                <i class="fas fa-signature"></i>
                ${info.signed ? 'Re-sign with current key' : 'Sign with current key'}
            </button>
            <small class="text-muted">${invalid
                ? 'The current signature does not verify — re-signing with the server key restores a valid one.'
                : 'Add an RSA-PSS signature using the server key.'}</small>
        </div>`;
}

async function resignFile(fileId) {
    const btn = document.getElementById('resignBtn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Signing…';
    }

    try {
        const response = await fetch(`/api/files/${fileId}/resign`, { method: 'POST' });
        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        showNotification('Metadata re-signed with the current key', 'success');
        await loadFiles();       // refresh the list badges
        showFileInfo(fileId);    // re-render the info modal

    } catch (error) {
        showNotification('Re-sign failed: ' + error.message, 'error');
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-signature"></i> Re-sign with current key';
        }
    }
}

// ============================================================
// VERIFY  (CLI: verify-metadata)
// ============================================================

function closeVerifyModal() {
    document.getElementById('verifyModal').classList.remove('show');
}

async function verifyFile(fileId) {
    const container = document.getElementById('verifyContent');
    container.innerHTML = '<p class="modal-note">Verifying...</p>';
    document.getElementById('verifyModal').classList.add('show');

    try {
        const response = await fetch(`/api/files/${fileId}/verify`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ strict: isStrict() })
        });

        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        const report = data.report;

        const checks = report.checks.map(check => `
            <li class="${check.ok ? 'ok' : (check.fatal || report.strict ? 'fail' : 'warn')}">
                <i class="fas ${check.ok
                    ? 'fa-circle-check'
                    : (check.fatal || report.strict ? 'fa-circle-xmark' : 'fa-triangle-exclamation')}"></i>
                <span>
                    <strong>${escapeHtml(check.name)}</strong>
                    <small>${escapeHtml(check.detail)}</small>
                </span>
            </li>
        `).join('');

        container.innerHTML = `
            <div class="verify-header ${report.passed ? 'passed' : 'failed'}">
                <i class="fas ${report.passed ? 'fa-shield-halved' : 'fa-shield-virus'}"></i>
                <div>
                    <strong>${report.passed ? 'Offline verification PASSED' : 'Offline verification FAILED'}</strong>
                    <small>${escapeHtml(report.filename)} · BSP ${report.protocol_version}
                        · strict mode ${report.strict ? 'on' : 'off'}</small>
                </div>
            </div>
            <ul class="check-list">${checks}</ul>
            <p class="modal-note">
                Offline checks do not contact the Library of Babel: they cannot prove the
                chunks are still retrievable, only that this metadata is authentic and
                well-formed.
            </p>
        `;

    } catch (error) {
        container.innerHTML =
            `<p class="verify-fail">Verification error: ${escapeHtml(error.message)}</p>`;
    }
}

// Verify a single chunk against the Merkle root (BSP v6).
// Fetches only that chunk from Babel — the one networked verify action.
async function verifyChunk(fileId, index, btn) {
    const row = document.querySelector(`tr[data-chunk-row="${index}"]`);
    const cell = row ? row.querySelector('.chunk-verify-cell') : null;

    if (btn) btn.disabled = true;
    if (cell) cell.innerHTML = '<i class="fas fa-spinner fa-spin text-muted"></i>';

    try {
        const response = await fetch(`/api/files/${fileId}/verify-chunk`, {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ index })
        });

        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        const r = data.report;

        if (cell) {
            cell.innerHTML = r.authentic
                ? '<span class="badge signed"><i class="fas fa-circle-check"></i> authentic</span>'
                : '<span class="badge warn"><i class="fas fa-circle-xmark"></i> failed</span>';
        }

        showNotification(
            r.authentic
                ? `Chunk ${index} is authentic — Merkle proof verified (${r.proof_length} hashes, no full download)`
                : `Chunk ${index} FAILED verification — retrieved data does not match the Merkle root`,
            r.authentic ? 'success' : 'error'
        );

    } catch (error) {
        if (cell) {
            cell.innerHTML = `<button class="action-btn chunk-verify"
                onclick="verifyChunk('${fileId}', ${index}, this)"
                title="Retry"><i class="fas fa-rotate-right"></i></button>`;
        }
        showNotification(`Chunk ${index} verification error: ${error.message}`, 'error');
    }
}

// ============================================================
// METADATA EXPORT / IMPORT
// ============================================================

function exportMetadata(fileId) {
    window.location.href = `/api/files/${fileId}/metadata`;
    showNotification('Exporting metadata — keep this file safe, it is the only way back', 'info');
}

function openImportModal() {
    document.getElementById('importResult').innerHTML = '';
    document.getElementById('importInput').value = '';
    document.getElementById('importModal').classList.add('show');
}

function closeImportModal() {
    document.getElementById('importModal').classList.remove('show');
}

async function importMetadata(file) {
    const result = document.getElementById('importResult');
    result.innerHTML = '<p class="modal-note">Importing...</p>';

    try {
        const formData = new FormData();
        formData.append('file', file);

        const response = await fetch('/api/metadata/import', {
            method: 'POST',
            body: formData
        });

        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        const warning = data.missing_coordinates
            ? `<p class="text-warn"><i class="fas fa-triangle-exclamation"></i>
                 ${data.missing_coordinates} chunk(s) have no coordinates — this file cannot be restored.</p>`
            : '';

        result.innerHTML = `
            <div class="verify-header passed">
                <i class="fas fa-circle-check"></i>
                <div>
                    <strong>Imported ${escapeHtml(data.filename)}</strong>
                    <small>${data.chunks} chunks · ${data.signed ? 'signed' : 'not signed'}</small>
                </div>
            </div>
            ${warning}
        `;

        loadFiles();
        showNotification('Metadata imported', 'success');

    } catch (error) {
        result.innerHTML =
            `<p class="verify-fail">Import failed: ${escapeHtml(error.message)}</p>`;
    }
}

// ============================================================
// DELETE
// ============================================================

async function deleteFile(fileId) {
    const file = allFiles.find(f => f.id === fileId);
    const name = file ? file.name : 'this file';

    if (!confirm(
        `Delete the metadata for "${name}"?\n\n` +
        `The chunks stay in the Library of Babel forever, but without these ` +
        `coordinates they can never be found again. Export the metadata first ` +
        `if you may want the file back.`
    )) {
        return;
    }

    try {
        const response = await fetch(`/api/delete/${fileId}`, {
            method: 'DELETE'
        });

        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        loadFiles();
        showNotification('Metadata deleted', 'success');

    } catch (error) {
        showNotification('Delete error: ' + error.message, 'error');
    }
}

// ============================================================
// SETTINGS / HELP
// ============================================================

function closeSettingsModal() {
    document.getElementById('settingsModal').classList.remove('show');
}

function closeHelpModal() {
    document.getElementById('helpModal').classList.remove('show');
}

function openHelpModal() {
    document.getElementById('helpModal').classList.add('show');
}

async function openSettingsModal() {
    const container = document.getElementById('settingsContent');
    document.getElementById('settingsModal').classList.add('show');

    await loadConfig();

    const yesNo = (value, yes, no) => value
        ? `<span class="badge signed">${yes}</span>`
        : `<span class="badge warn">${no}</span>`;

    const lim = serverConfig.limits || {};
    const bound = (k, i, d) => (lim[k] && lim[k][i] !== undefined) ? lim[k][i] : d;
    const mb = Math.round(serverConfig.max_file_size / (1024 * 1024));
    const minMb = Math.max(1, Math.floor(bound('max_file_size', 0, 1048576) / (1024 * 1024)));
    const maxMb = Math.floor(bound('max_file_size', 1, 2147483648) / (1024 * 1024));

    container.innerHTML = `
        <h4 class="section-title" style="margin-top:0">Editable settings</h4>
        <div class="settings-form">
            <label class="setting-row">
                <span>Rate limit <small>seconds between chunks — 0 disables</small></span>
                <input type="number" id="setRate" step="0.5"
                    min="${bound('rate_limit_delay', 0, 0)}" max="${bound('rate_limit_delay', 1, 60)}"
                    value="${serverConfig.rate_limit_delay}">
            </label>
            <label class="setting-row">
                <span>Max upload size <small>MB</small></span>
                <input type="number" id="setMaxMb" step="1"
                    min="${minMb}" max="${maxMb}" value="${mb}">
            </label>
            <label class="setting-row">
                <span>Retries per chunk <small>attempts before giving up</small></span>
                <input type="number" id="setRetries" step="1"
                    min="${bound('max_retries', 0, 1)}" max="${bound('max_retries', 1, 10)}"
                    value="${serverConfig.max_retries}">
            </label>
            <label class="setting-row">
                <span>Retry backoff <small>initial seconds, doubles each try</small></span>
                <input type="number" id="setRetryDelay" step="0.5"
                    min="${bound('retry_delay', 0, 0)}" max="${bound('retry_delay', 1, 30)}"
                    value="${serverConfig.retry_delay}">
            </label>
            <label class="setting-row toggle-row">
                <span>Strict mode by default <small>abort on any integrity failure</small></span>
                <input type="checkbox" id="setStrict" ${serverConfig.default_strict ? 'checked' : ''}>
            </label>
        </div>
        <button class="primary-btn" id="saveSettingsBtn" onclick="saveSettings()">
            <i class="fas fa-floppy-disk"></i> Save settings
        </button>
        <p class="modal-note">
            Saved to <code>${escapeHtml(serverConfig.config_file)}</code> and applied
            immediately. Startup-only options (host, port, key paths) are set via
            environment variables or the CLI — see Help.
        </p>

        <h4 class="section-title">Server info</h4>
        <table class="kv-table">
            <tr><th>Protocol</th><td>BSP ${serverConfig.protocol_version}</td></tr>
            <tr><th>Chunk size</th><td>${serverConfig.max_chunk_bytes} bytes before encoding</td></tr>
            <tr><th>Encoding overhead</th><td>×${serverConfig.encoding_overhead}
                (base-29, ${serverConfig.max_search_length} chars per Babel page)</td></tr>
            <tr><th>Private key</th>
                <td>${yesNo(serverConfig.has_private_key, 'found', 'not found')}
                    <code>${escapeHtml(serverConfig.private_key_path)}</code></td></tr>
            <tr><th>Public key</th>
                <td>${yesNo(serverConfig.has_public_key, 'found', 'not found')}
                    <code>${escapeHtml(serverConfig.public_key_path)}</code></td></tr>
        </table>

        <div class="key-actions">
            ${serverConfig.has_private_key ? `
                <p class="modal-note">
                    <i class="fas fa-circle-check text-ok"></i>
                    Key pair present — you can sign uploads and verify signatures.
                </p>
                <button class="secondary-btn" id="genKeysBtn" onclick="generateKeys(false)">
                    <i class="fas fa-rotate"></i> Regenerate key pair
                </button>
                <p class="modal-note text-warn">
                    <i class="fas fa-triangle-exclamation"></i>
                    Regenerating replaces the current key. Every metadata already
                    signed with it becomes permanently unverifiable.
                </p>
            ` : `
                <button class="primary-btn" id="genKeysBtn" onclick="generateKeys(false)">
                    <i class="fas fa-key"></i> Generate RSA-4096 key pair
                </button>
                <p class="modal-note">
                    Creates <code>${escapeHtml(serverConfig.private_key_path)}</code>
                    and <code>${escapeHtml(serverConfig.public_key_path)}</code> on the
                    server. The private key never leaves the server — keep it safe and
                    never commit it.
                </p>
            `}
        </div>
    `;
}

// Persist the editable settings from the Settings modal.
async function saveSettings() {
    const btn = document.getElementById('saveSettingsBtn');
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Saving…';
    }

    const payload = {
        rate_limit_delay: parseFloat(document.getElementById('setRate').value),
        max_file_size: Math.round(parseFloat(document.getElementById('setMaxMb').value) * 1024 * 1024),
        max_retries: parseInt(document.getElementById('setRetries').value, 10),
        retry_delay: parseFloat(document.getElementById('setRetryDelay').value),
        default_strict: document.getElementById('setStrict').checked,
    };

    try {
        const response = await fetch('/api/settings', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify(payload)
        });

        const data = await response.json();

        if (!response.ok || !data.success) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        showNotification('Settings saved', 'success');
        // loadConfig refreshes serverConfig, the max-size label and the
        // toolbar strict toggle to the new default; then re-render the modal.
        await loadConfig();
        await openSettingsModal();

    } catch (error) {
        showNotification('Could not save settings: ' + error.message, 'error');
        if (btn) {
            btn.disabled = false;
            btn.innerHTML = '<i class="fas fa-floppy-disk"></i> Save settings';
        }
    }
}

// Generate an RSA-4096 key pair on the server (Settings modal).
// The private key stays on the server; overwriting an existing key needs
// an explicit confirmation because it invalidates prior signatures.
async function generateKeys(force) {
    const btn = document.getElementById('genKeysBtn');
    const original = btn ? btn.innerHTML : '';
    if (btn) {
        btn.disabled = true;
        btn.innerHTML = '<i class="fas fa-spinner fa-spin"></i> Generating…';
    }

    try {
        const response = await fetch('/api/keys/generate', {
            method: 'POST',
            headers: { 'Content-Type': 'application/json' },
            body: JSON.stringify({ force: !!force })
        });

        const data = await response.json();

        // Server refuses to overwrite an existing key without confirmation.
        if (response.status === 409 && data.requires_force) {
            if (confirm(data.error + '\n\nOverwrite the existing key anyway?')) {
                return generateKeys(true);
            }
            if (btn) { btn.disabled = false; btn.innerHTML = original; }
            return;
        }

        if (!response.ok || !data.success) {
            throw new Error(data.error || `HTTP ${response.status}`);
        }

        showNotification('RSA-4096 key pair generated', 'success');
        // Refresh config (re-enables the sign checkbox) and re-render Settings.
        await openSettingsModal();

    } catch (error) {
        showNotification('Key generation failed: ' + error.message, 'error');
        if (btn) { btn.disabled = false; btn.innerHTML = original; }
    }
}

// ============================================================
// STORAGE INFO
// ============================================================

function updateStorageInfo() {
    const totalSize = allFiles.reduce((sum, file) => sum + (file.size_bytes || 0), 0);
    const totalChunks = allFiles.reduce((sum, file) => sum + (file.chunks || 0), 0);

    // The library is infinite; the bar tracks the largest file set seen so far
    const percent = Math.min((totalSize / (1024 * 1024 * 100)) * 100, 100);

    document.getElementById('storageUsed').textContent = formatFileSize(totalSize);
    document.querySelector('.storage-used').style.width = percent + '%';
    document.querySelector('.storage-note').textContent =
        `${totalChunks.toLocaleString()} chunks located, 0 bytes stored`;
}

// ============================================================
// UTILITIES
// ============================================================

function setText(id, text) {
    const el = document.getElementById(id);
    if (el) el.textContent = text;
}

function formatFileSize(bytes) {
    if (!bytes) return '0 B';
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
    div.textContent = text === undefined || text === null ? '' : text;
    return div.innerHTML;
}

function showNotification(message, type = 'success') {
    const notification = document.getElementById('notification');
    const icon = notification.querySelector('i');

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
    }, 4000);
}
