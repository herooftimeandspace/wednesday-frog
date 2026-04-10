function getCsrfToken() {
  const meta = document.querySelector('meta[name="csrf-token"]');
  return meta ? meta.content : '';
}

function timeoutLoginUrl() {
  const value = document.body.dataset.timeoutLoginUrl;
  return value || '/login?reason=timeout';
}

function logoutUrl() {
  const value = document.body.dataset.logoutUrl;
  return value || '/logout';
}

function redirectToTimeoutLogin() {
  window.location.assign(timeoutLoginUrl());
}

async function postJson(url, targetId) {
  const token = getCsrfToken();
  const target = document.getElementById(targetId);
  if (target) {
    target.textContent = 'Working...';
  }
  try {
    const response = await fetch(url, {
      method: 'POST',
      headers: { 'X-CSRF-Token': token }
    });
    const text = await response.text();
    let payload;
    try {
      payload = JSON.parse(text);
    } catch (error) {
      payload = {
        status: response.status,
        ok: response.ok,
        body: text
      };
    }
    if (response.status === 401 && payload && payload.reason === 'timeout') {
      redirectToTimeoutLogin();
      return;
    }
    if (target) {
      target.textContent = JSON.stringify(payload, null, 2);
    }
  } catch (error) {
    if (target) {
      target.textContent = error.message;
    }
  }
}

function initializeEphemeralFlashes() {
  const flashes = document.querySelectorAll('[data-auto-dismiss-ms]');
  flashes.forEach((flash) => {
    const timeoutMs = Number.parseInt(flash.getAttribute('data-auto-dismiss-ms') || '', 10);
    if (!Number.isFinite(timeoutMs) || timeoutMs < 1) {
      return;
    }
    window.setTimeout(() => {
      flash.classList.add('is-dismissing');
      window.setTimeout(() => {
        flash.remove();
      }, 220);
    }, timeoutMs);
  });
}

function initializeIdleLogout() {
  if (document.body.dataset.authenticated !== 'true') {
    return;
  }
  const idleTimeoutMs = Number.parseInt(document.body.dataset.idleTimeoutMs || '', 10);
  if (!Number.isFinite(idleTimeoutMs) || idleTimeoutMs < 1) {
    return;
  }

  let timerId = null;
  let logoutPending = false;

  const scheduleLogout = () => {
    if (timerId !== null) {
      window.clearTimeout(timerId);
    }
    timerId = window.setTimeout(async () => {
      if (logoutPending) {
        return;
      }
      logoutPending = true;
      try {
        const body = new URLSearchParams({ csrf_token: getCsrfToken() });
        await fetch(logoutUrl(), {
          method: 'POST',
          headers: { 'Content-Type': 'application/x-www-form-urlencoded' },
          body
        });
      } catch (error) {
        // Ignore network errors and still redirect to the timeout login page.
      }
      redirectToTimeoutLogin();
    }, idleTimeoutMs);
  };

  const resetTimer = () => {
    if (logoutPending) {
      return;
    }
    scheduleLogout();
  };

  ['pointerdown', 'keydown', 'scroll', 'touchstart', 'focus'].forEach((eventName) => {
    window.addEventListener(eventName, resetTimer, { passive: true });
  });

  document.addEventListener('visibilitychange', () => {
    if (document.visibilityState === 'visible') {
      resetTimer();
    }
  });

  scheduleLogout();
}

document.addEventListener('click', (event) => {
  if (!(event.target instanceof Element)) {
    return;
  }
  const button = event.target.closest('[data-post-json-url]');
  if (!button) {
    return;
  }
  event.preventDefault();
  const url = button.getAttribute('data-post-json-url');
  const targetId = button.getAttribute('data-post-json-target');
  if (!url || !targetId) {
    return;
  }
  postJson(url, targetId);
});

document.addEventListener('change', (event) => {
  if (!(event.target instanceof HTMLSelectElement)) {
    return;
  }
  const control = event.target;
  const selectedOption = control.selectedOptions.length > 0 ? control.selectedOptions[0] : null;
  const displayLabel = selectedOption ? selectedOption.textContent : control.value;
  const timezoneLabelTarget = control.getAttribute('data-timezone-label-target');
  if (timezoneLabelTarget) {
    const labelNode = document.getElementById(timezoneLabelTarget);
    if (labelNode) {
      labelNode.textContent = displayLabel || control.value;
    }
  }
  if (control.getAttribute('data-auto-submit-on-change') !== 'true') {
    return;
  }
  const form = control.form;
  if (!form) {
    return;
  }
  form.requestSubmit();
});

initializeEphemeralFlashes();
initializeIdleLogout();
