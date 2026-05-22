const form = document.getElementById('message-form');
const input = document.getElementById('message-input');
const button = document.getElementById('send-button');
const statusEl = document.getElementById('status');
const listEl = document.getElementById('message-list');
const emptyState = document.getElementById('empty-state');
const countEl = document.getElementById('message-count');

let messageCount = 0;

function formatDate(isoString) {
  const date = new Date(isoString);
  return Number.isNaN(date.getTime()) ? 'Unknown time' : date.toLocaleString();
}

function syncEmptyState() {
  emptyState.hidden = messageCount > 0;
  countEl.textContent = String(messageCount);
}

function createMessageNode(message) {
  const item = document.createElement('li');
  item.className = 'message-item';

  const text = document.createElement('p');
  text.className = 'message-text';
  text.textContent = message.text;

  const meta = document.createElement('span');
  meta.className = 'message-meta';
  meta.textContent = `${formatDate(message.createdAt)} · ${message.id}`;

  item.append(text, meta);
  return item;
}

function renderMessages(messages) {
  listEl.innerHTML = '';

  for (const message of messages) {
    listEl.appendChild(createMessageNode(message));
  }

  messageCount = messages.length;
  syncEmptyState();
}

function appendMessage(message) {
  listEl.appendChild(createMessageNode(message));
  messageCount += 1;
  syncEmptyState();
  listEl.lastElementChild?.scrollIntoView({ behavior: 'smooth', block: 'end' });
}

async function loadMessages() {
  const response = await fetch('/api/messages');
  if (!response.ok) {
    throw new Error('Impossible de charger les messages.');
  }

  const messages = await response.json();
  renderMessages(messages);
}

function connectStream() {
  const stream = new EventSource('/events');

  stream.addEventListener('open', () => {
    statusEl.textContent = 'Connecté à RabbitMQ.';
  });

  stream.addEventListener('snapshot', (event) => {
    try {
      const snapshot = JSON.parse(event.data);
      renderMessages(snapshot);
    } catch (error) {
      console.warn('Snapshot event ignored:', error);
    }
  });

  stream.addEventListener('message', (event) => {
    try {
      const message = JSON.parse(event.data);
      appendMessage(message);
      statusEl.textContent = 'Nouveau message reçu.';
    } catch (error) {
      console.warn('Message event ignored:', error);
    }
  });

  stream.addEventListener('error', () => {
    statusEl.textContent = 'Flux momentanément indisponible, nouvelle tentative en cours...';
  });
}

form.addEventListener('submit', async (event) => {
  event.preventDefault();

  const value = input.value.trim();
  if (!value) {
    return;
  }

  button.disabled = true;
  statusEl.textContent = 'Envoi vers RabbitMQ...';

  try {
    const response = await fetch('/api/messages', {
      method: 'POST',
      headers: {
        'Content-Type': 'application/json',
      },
      body: JSON.stringify({ message: value }),
    });

    if (!response.ok) {
      const error = await response.json().catch(() => ({}));
      throw new Error(error.error || 'Impossible d’envoyer le message.');
    }

    input.value = '';
    input.focus();
    statusEl.textContent = 'Message envoyé. En attente de consommation...';
  } catch (error) {
    statusEl.textContent = error.message;
  } finally {
    button.disabled = false;
  }
});

loadMessages()
  .catch((error) => {
    statusEl.textContent = error.message;
  })
  .finally(() => {
    connectStream();
  });
