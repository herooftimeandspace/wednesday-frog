async function postJson(url, targetId) {
  const meta = document.querySelector('meta[name="csrf-token"]');
  const token = meta ? meta.content : '';
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
    if (target) {
      target.textContent = JSON.stringify(payload, null, 2);
    }
  } catch (error) {
    if (target) {
      target.textContent = error.message;
    }
  }
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
