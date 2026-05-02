// Navigation Logic
const navBtns = document.querySelectorAll('.nav-btn');
const sections = document.querySelectorAll('section');

navBtns.forEach(btn => {
    btn.addEventListener('click', () => {
        // Remove active class from all buttons and sections
        navBtns.forEach(b => b.classList.remove('active'));
        sections.forEach(s => s.classList.remove('active-section'));

        // Add active class to clicked button and target section
        btn.classList.add('active');
        document.getElementById(btn.dataset.target).classList.add('active-section');
    });
});

// Chart.js Default Config for Dark Mode
Chart.defaults.color = '#adb5bd';
Chart.defaults.borderColor = 'rgba(255, 255, 255, 0.1)';

const accentColors = [
    'rgba(0, 242, 254, 0.8)',
    'rgba(79, 172, 254, 0.8)',
    'rgba(138, 43, 226, 0.8)',
    'rgba(255, 0, 128, 0.8)',
    'rgba(0, 201, 143, 0.8)',
    'rgba(255, 165, 0, 0.8)'
];

// Load Analytics Data
async function loadAnalytics() {
    try {
        const response = await fetch('../outputs/analytics.json');
        if (!response.ok) throw new Error('Network response was not ok');
        const data = await response.json();
        
        renderKPIs(data);
        renderCharts(data);
    } catch (error) {
        console.error('Error loading analytics:', error);
    }
}

function renderKPIs(data) {
    const container = document.getElementById('kpi-container');
    container.innerHTML = `
        <div class="kpi-card glass-card">
            <h4>Total Destinations</h4>
            <div class="value">${data.n_places.toLocaleString()}</div>
        </div>
        <div class="kpi-card glass-card">
            <h4>Districts Covered</h4>
            <div class="value">${data.district_summary.length}</div>
        </div>
        <div class="kpi-card glass-card">
            <h4>Top Category</h4>
            <div class="value">${data.type_distribution[0].Type}</div>
        </div>
    `;
}

function renderCharts(data) {
    // Type Distribution Chart (Doughnut)
    const typeCtx = document.getElementById('typeChart').getContext('2d');
    const topTypes = data.type_distribution.slice(0, 6);
    new Chart(typeCtx, {
        type: 'doughnut',
        data: {
            labels: topTypes.map(d => d.Type),
            datasets: [{
                data: topTypes.map(d => d.count),
                backgroundColor: accentColors,
                borderWidth: 0
            }]
        },
        options: {
            responsive: true,
            plugins: {
                legend: { position: 'right' }
            }
        }
    });

    // Tag Frequency Chart (Bar)
    const tagCtx = document.getElementById('tagChart').getContext('2d');
    const topTags = data.tag_frequency.slice(0, 8);
    new Chart(tagCtx, {
        type: 'bar',
        data: {
            labels: topTags.map(d => d.tag),
            datasets: [{
                label: 'Frequency',
                data: topTags.map(d => d.count),
                backgroundColor: 'rgba(0, 242, 254, 0.6)',
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
                y: { beginAtZero: true }
            }
        }
    });

    // District Distribution Chart (Bar)
    const distCtx = document.getElementById('districtChart').getContext('2d');
    const topDistricts = data.district_distribution.slice(0, 15);
    new Chart(distCtx, {
        type: 'bar',
        data: {
            labels: topDistricts.map(d => d.District),
            datasets: [{
                label: 'Number of Places',
                data: topDistricts.map(d => d.count),
                backgroundColor: 'rgba(138, 43, 226, 0.6)',
                borderRadius: 4
            }]
        },
        options: {
            responsive: true,
            plugins: { legend: { display: false } },
            scales: {
                y: { beginAtZero: true }
            }
        }
    });
}

// Data fetching helper for static JSON
let cachedRecommendations = null;
async function fetchRecommendationsData() {
    if (cachedRecommendations) return cachedRecommendations;
    const response = await fetch('../outputs/recommendations.json');
    if (!response.ok) throw new Error('Network response was not ok');
    cachedRecommendations = await response.json();
    return cachedRecommendations;
}

// Load Recommendation Data
async function initRecommendations() {
    const userSelect = document.getElementById('userSelect');
    
    try {
        const data = await fetchRecommendationsData();
        const users = Object.keys(data).map(Number).sort((a,b) => a - b);
        
        userSelect.innerHTML = '<option value="">-- Choose User ID --</option>';
        users.forEach(uid => {
            const opt = document.createElement('option');
            opt.value = uid;
            opt.textContent = `User ${uid}`;
            userSelect.appendChild(opt);
        });
        
        userSelect.addEventListener('change', (e) => {
            if (e.target.value) {
                loadUserRecommendations(e.target.value);
            } else {
                document.getElementById('recommendation-container').innerHTML = '';
            }
        });
        
    } catch (error) {
        console.error('Error:', error);
        userSelect.innerHTML = '<option value="">Error loading users</option>';
    }
}

async function loadUserRecommendations(userId) {
    const container = document.getElementById('recommendation-container');
    const spinner = document.getElementById('loading-spinner');
    
    container.innerHTML = '';
    spinner.classList.remove('hidden');
    
    try {
        const data = await fetchRecommendationsData();
        const recs = data[userId];
        
        if (!recs) throw new Error('User not found');
        
        spinner.classList.add('hidden');
        
        recs.forEach((rec, index) => {
            const card = document.createElement('div');
            card.className = 'rec-card glass-card';
            
            // Format score to 2 decimal places
            const score = parseFloat(rec.pred_rating).toFixed(2);
            
            // Create tags HTML
            let tagsHtml = '';
            if (rec.Tags) {
                const tagsList = rec.Tags.split(' ').filter(t => t);
                tagsHtml = tagsList.slice(0, 4).map(t => `<span class="tag">${t}</span>`).join('');
            }
            
            card.innerHTML = `
                <div class="score-badge">★ ${score}</div>
                <h3>${rec.Name || 'Unknown Place'}</h3>
                <p>📍 ${rec.City || 'Unknown'}, ${rec.District}</p>
                <p>🏷️ ${rec.Type}</p>
                <div class="rec-tags">
                    ${tagsHtml}
                </div>
            `;
            
            container.appendChild(card);
            
            // Stagger animation
            card.style.animation = `fadeIn 0.5s ease forwards ${index * 0.05}s`;
            card.style.opacity = '0';
        });
        
    } catch (error) {
        spinner.classList.add('hidden');
        container.innerHTML = `<p style="color: #ff4d4d; text-align: center; grid-column: 1/-1;">Error loading recommendations for this user.</p>`;
    }
}

// Initialize
document.addEventListener('DOMContentLoaded', () => {
    loadAnalytics();
    initRecommendations();
});
