// Job Scraper Frontend JavaScript

document.addEventListener('DOMContentLoaded', function() {
    // Run Scraper buttons
    const runScraperBtns = document.querySelectorAll('#runScraper, #runScraperEmpty');
    const loadingOverlay = document.getElementById('loadingOverlay');
    
    runScraperBtns.forEach(btn => {
        btn.addEventListener('click', async function() {
            if (!confirm('Run the job scraper? This may take a few minutes.')) {
                return;
            }
            
            // Show loading overlay
            loadingOverlay.classList.remove('hidden');
            
            try {
                const response = await fetch('/run-scraper');
                const data = await response.json();
                
                if (data.status === 'success') {
                    alert('Scraper completed successfully!');
                    window.location.reload();
                } else {
                    alert('Scraper error: ' + (data.message || data.output || 'Unknown error'));
                }
            } catch (error) {
                alert('Error running scraper: ' + error.message);
            } finally {
                loadingOverlay.classList.add('hidden');
            }
        });
    });
    
    // Auto-submit filters on change
    const filterSelects = document.querySelectorAll('.filter-select');
    filterSelects.forEach(select => {
        select.addEventListener('change', function() {
            this.closest('form').submit();
        });
    });
    
    // Search debounce
    let searchTimeout;
    const searchInput = document.querySelector('.search-input');
    if (searchInput) {
        searchInput.addEventListener('input', function() {
            clearTimeout(searchTimeout);
            searchTimeout = setTimeout(() => {
                if (this.value.length >= 3 || this.value.length === 0) {
                    this.closest('form').submit();
                }
            }, 500);
        });
    }
    
    // Keyboard shortcuts
    document.addEventListener('keydown', function(e) {
        // Press '/' to focus search
        if (e.key === '/' && !e.ctrlKey && !e.metaKey) {
            e.preventDefault();
            searchInput?.focus();
        }
        
        // Press 'Escape' to clear search
        if (e.key === 'Escape' && searchInput === document.activeElement) {
            searchInput.value = '';
            searchInput.blur();
        }
    });
    
    // Add hover effects to job cards
    const jobCards = document.querySelectorAll('.job-card');
    jobCards.forEach(card => {
        card.addEventListener('mouseenter', function() {
            this.style.transform = 'translateY(-4px)';
        });
        card.addEventListener('mouseleave', function() {
            this.style.transform = 'translateY(0)';
        });
    });
    
    // Animate stats on load
    const statValues = document.querySelectorAll('.stat-value');
    statValues.forEach(stat => {
        const finalValue = parseInt(stat.textContent) || 0;
        let currentValue = 0;
        const duration = 1000;
        const increment = finalValue / (duration / 16);
        
        function updateValue() {
            currentValue += increment;
            if (currentValue < finalValue) {
                stat.textContent = Math.floor(currentValue);
                requestAnimationFrame(updateValue);
            } else {
                stat.textContent = finalValue;
            }
        }
        
        if (finalValue > 0) {
            stat.textContent = '0';
            setTimeout(updateValue, 200);
        }
    });
});

// Refresh stats periodically
setInterval(async function() {
    try {
        const response = await fetch('/api/stats');
        if (response.ok) {
            const stats = await response.json();
            // Update stats in sidebar
            const statValues = document.querySelectorAll('.stat-value');
            if (statValues[0]) statValues[0].textContent = stats.job_count;
            if (statValues[1]) statValues[1].textContent = stats.company_count;
            if (statValues[2]) statValues[2].textContent = stats.high_score_count;
        }
    } catch (error) {
        console.error('Error refreshing stats:', error);
    }
}, 30000); // Every 30 seconds
