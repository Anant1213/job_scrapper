// static/js/control-panel.js
// CV Upload and Actions Control Panel JavaScript

(function () {
    const cvForm = document.getElementById('cv-upload-form');
    const cvFileInput = document.getElementById('cv-file-input');
    const uploadBtn = document.getElementById('upload-cv-btn');
    const matchBtn = document.getElementById('match-jobs-btn');
    const scraperBtn = document.getElementById('run-scraper-btn');
    const progressSection = document.getElementById('progress-section');
    const progressBar = document.getElementById('progress-bar');
    const progressText = document.getElementById('progress-text');
    const progressPercent = document.getElementById('progress-percent');
    const progressDetail = document.getElementById('progress-detail');
    const cvInfo = document.getElementById('cv-info');
    const cvStatusBadge = document.getElementById('cv-status-badge');

    // Check CV status on load
    checkCVStatus();

    function check CVStatus() {
        fetch('/api/cv-status')
            .then(r => r.json())
            .then(data => {
                if (data.has_cv) {
                    cvStatusBadge.style.display = 'block';
                    matchBtn.disabled = false;
                    matchBtn.style.opacity = '1';

                    // Show CV info
                    cvInfo.style.display = 'block';
                    document.getElementById('cv-filename').textContent = data.filename;

                    try {
                        const skills = JSON.parse(data.skills || '[]');
                        document.getElementById('cv-skills').textContent = skills.slice(0, 5).join(', ') || 'Not extracted';
                    } catch {
                        document.getElementById('cv-skills').textContent = 'Not extracted';
                    }

                    document.getElementById('cv-experience').textContent = data.experience || 0;
                }
            }).catch(err => console.error('Error checking CV status:', err));
    }

    // CV Upload
    cvForm.addEventListener('submit', async (e) => {
        e.preventDefault();

        const file = cvFileInput.files[0];
        if (!file) {
            alert('Please select a CV file');
            return;
        }

        const formData = new FormData();
        formData.append('cv_file', file);

        uploadBtn.disabled = true;
        uploadBtn.textContent = '⏳ Uploading...';
        showProgress('Uploading CV...', 0);

        try {
            const response = await fetch('/upload-cv', {
                method: 'POST',
                body: formData
            });

            const data = await response.json();

            if (data.success) {
                showProgress('Analyzing CV with AI...', 30);
                pollTaskStatus(data.task_id, 'cv');
            } else {
                alert('Error: ' + (data.error || 'Upload failed'));
                hideProgress();
                uploadBtn.disabled = false;
                uploadBtn.textContent = '📤 Upload & Analyze CV';
            }
        } catch (error) {
            alert('Error uploading CV: ' + error);
            hideProgress();
            uploadBtn.disabled = false;
            uploadBtn.textContent = '📤 Upload & Analyze CV';
        }
    });

    // Match Jobs
    matchBtn.addEventListener('click', async () => {
        if (!confirm('Match all jobs with your CV using AI? This takes 5-10 minutes.')) {
            return;
        }

        matchBtn.disabled = true;
        matchBtn.textContent = '⏳ Matching...';
        showProgress('Starting job matching...', 0);

        try {
            const response = await fetch('/match-jobs', { method: 'POST' });
            const data = await response.json();

            if (data.success) {
                pollTaskStatus(data.task_id, 'match');
            } else {
                alert('Error: ' + (data.error || 'Matching failed'));
                hideProgress();
                matchBtn.disabled = false;
                matchBtn.textContent = '🎯 Match Jobs with AI';
            }
        } catch (error) {
            alert('Error: ' + error);
            hideProgress();
            matchBtn.disabled = false;
            matchBtn.textContent = '🎯 Match Jobs with AI';
        }
    });

    // Run Scraper
    scraperBtn.addEventListener('click', async () => {
        if (!confirm('Scrape jobs from official websites? This takes 5-10 minutes.')) {
            return;
        }

        scraperBtn.disabled = true;
        scraperBtn.textContent = '⏳ Scraping...';
        showProgress('Starting scraper...', 0);

        try {
            const response = await fetch('/run-scraper', { method: 'POST' });
            const data = await response.json();

            if (data.success) {
                pollTaskStatus(data.task_id, 'scrape');
            } else {
                alert('Error: ' + (data.error || 'Scraping failed'));
                hideProgress();
                scraperBtn.disabled = false;
                scraperBtn.textContent = '🔄 Scrape New Jobs';
            }
        } catch (error) {
            alert('Error: ' + error);
            hideProgress();
            scraperBtn.disabled = false;
            scraperBtn.textContent = '🔄 Scrape New Jobs';
        }
    });

    function pollTaskStatus(taskId, type) {
        const interval = setInterval(async () => {
            try {
                const response = await fetch(`/api/task-status/${taskId}`);
                const data = await response.json();

                if (data.error) {
                    clearInterval(interval);
                    alert('Task error: ' + data.error);
                    hideProgress();
                    resetButtons(type);
                    return;
                }

                if (data.status === 'complete') {
                    clearInterval(interval);
                    handleTaskComplete(type, data);
                } else if (data.status === 'error') {
                    clearInterval(interval);
                    alert('Error: ' + data.progress);
                    hideProgress();
                    resetButtons(type);
                } else {
                    updateProgress(type, data);
                }
            } catch (error) {
                clearInterval(interval);
                alert('Error checking status: ' + error);
                hideProgress();
                resetButtons(type);
            }
        }, 2000);
    }

    function updateProgress(type, data) {
        if (type === 'cv') {
            showProgress(data.progress, 70);
        } else if (type === 'match') {
            const percent = data.total > 0 ? (data.matched / data.total * 100) : 0;
            showProgress(data.progress, percent);
            progressDetail.textContent = `High matches (70+): ${data.high_matches}`;
        } else if (type === 'scrape') {
            showProgress(data.progress, 50);
        }
    }

    function handleTaskComplete(type, data) {
        if (type === 'cv') {
            showProgress('✅ CV Analysis Complete!', 100);
            setTimeout(() => {
                hideProgress();
                checkCVStatus();
                uploadBtn.disabled = false;
                uploadBtn.textContent = '📤 Upload & Analyze CV';
            }, 2000);
        } else if (type === 'match') {
            showProgress(`✅ Complete! ${data.high_matches} high matches`, 100);
            progressDetail.textContent = `Matched ${data.matched}/${data.total} jobs`;
            setTimeout(() => window.location.reload(), 3000);
        } else if (type === 'scrape') {
            showProgress('✅ Scraping Complete!', 100);
            setTimeout(() => window.location.reload(), 2000);
        }
    }

    function resetButtons(type) {
        if (type === 'cv') {
            uploadBtn.disabled = false;
            uploadBtn.textContent = '📤 Upload & Analyze CV';
        } else if (type === 'match') {
            matchBtn.disabled = false;
            matchBtn.textContent = '🎯 Match Jobs with AI';
        } else if (type === 'scrape') {
            scraperBtn.disabled = false;
            scraperBtn.textContent = '🔄 Scrape New Jobs';
        }
    }

    function showProgress(text, percent) {
        progressSection.style.display = 'block';
        progressText.textContent = text;
        progressPercent.textContent = Math.round(percent) + '%';
        progressBar.style.width = percent + '%';
    }

    function hideProgress() {
        progressSection.style.display = 'none';
        progressDetail.textContent = '';
    }
})();
