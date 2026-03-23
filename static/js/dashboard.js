function showNewProjectModal() {
    document.getElementById('newProjectModal').style.display = 'flex';
    document.getElementById('projectName').focus();
}

function hideNewProjectModal() {
    document.getElementById('newProjectModal').style.display = 'none';
}

async function createProject(e) {
    e.preventDefault();
    const name = document.getElementById('projectName').value.trim();
    const address = document.getElementById('projectAddress').value.trim();
    if (!name) return;

    const res = await fetch('/api/projects', {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ name, address })
    });
    const data = await res.json();
    window.location.href = '/project/' + data.id;
}

async function deleteProject(e, id, name) {
    e.preventDefault();
    e.stopPropagation();
    if (!confirm('Delete project "' + name + '"? This cannot be undone.')) return;

    await fetch('/api/projects/' + id, { method: 'DELETE' });
    const card = document.querySelector('.project-card[data-id="' + id + '"]');
    if (card) card.remove();

    // Show empty state if no more projects
    const grid = document.getElementById('projectGrid');
    if (!grid.querySelector('.project-card')) {
        grid.innerHTML = '<div class="empty-state"><p>No projects yet. Create one to get started.</p></div>';
    }
}

// Close modal on escape key
document.addEventListener('keydown', function(e) {
    if (e.key === 'Escape') hideNewProjectModal();
});

// Close modal on overlay click
document.getElementById('newProjectModal').addEventListener('click', function(e) {
    if (e.target === this) hideNewProjectModal();
});
