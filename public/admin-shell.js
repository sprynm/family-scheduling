// Layout and accessibility layer for the admin console.
// Owns view routing, tabs and keyboard access only; all data loading and actions stay in admin.js.
(() => {
  const DEFAULT_VIEW = 'sources';
  const views = Array.from(document.querySelectorAll('[data-view]'));
  const viewLinks = Array.from(document.querySelectorAll('[data-view-link]'));
  // Only the console's own tab strips; the event planner manages its tabs itself.
  const tablists = Array.from(document.querySelectorAll('.tabs[role="tablist"]'));

  // --- Views and tabs, addressed by hash: #view or #view/tab ---

  function parseHash() {
    const [view = '', tab = ''] = location.hash.replace(/^#/, '').split('/');
    return {
      view: views.some((el) => el.dataset.view === view) ? view : DEFAULT_VIEW,
      tab,
    };
  }

  function tabsIn(tablist) {
    return Array.from(tablist.querySelectorAll('[role="tab"]'));
  }

  function selectTab(tab, { focus = false } = {}) {
    const tablist = tab.closest('[role="tablist"]');
    for (const other of tabsIn(tablist)) {
      const selected = other === tab;
      other.setAttribute('aria-selected', String(selected));
      other.tabIndex = selected ? 0 : -1;
      const panel = document.getElementById(other.getAttribute('aria-controls'));
      if (panel) panel.hidden = !selected;
    }
    if (focus) tab.focus();
  }

  function showView(viewName, tabName) {
    for (const view of views) view.hidden = view.dataset.view !== viewName;
    for (const link of viewLinks) {
      if (link.dataset.viewLink === viewName) link.setAttribute('aria-current', 'page');
      else link.removeAttribute('aria-current');
    }
    const view = views.find((el) => el.dataset.view === viewName);
    const tablist = view?.querySelector('.tabs[role="tablist"]');
    if (tablist) {
      const tabs = tabsIn(tablist);
      selectTab(tabs.find((tab) => tab.dataset.tab === tabName) || tabs.find((tab) => tab.getAttribute('aria-selected') === 'true') || tabs[0]);
    }
  }

  function writeHash(viewName, tabName) {
    const next = '#' + viewName + (tabName ? '/' + tabName : '');
    if (location.hash !== next) history.replaceState(null, '', next);
  }

  function openTab(viewName, tabName) {
    showView(viewName, tabName);
    writeHash(viewName, tabName);
  }

  for (const tablist of tablists) {
    const viewName = tablist.closest('[data-view]')?.dataset.view;
    tablist.addEventListener('click', (event) => {
      const tab = event.target.closest('[role="tab"]');
      if (!tab) return;
      selectTab(tab);
      writeHash(viewName, tab.dataset.tab);
    });
    tablist.addEventListener('keydown', (event) => {
      const tabs = tabsIn(tablist);
      const index = tabs.indexOf(document.activeElement);
      if (index < 0) return;
      const moves = { ArrowRight: index + 1, ArrowLeft: index - 1, Home: 0, End: tabs.length - 1 };
      if (!(event.key in moves)) return;
      event.preventDefault();
      const next = tabs[(moves[event.key] + tabs.length) % tabs.length];
      selectTab(next, { focus: true });
      writeHash(viewName, next.dataset.tab);
    });
  }

  function showViewFromHash() {
    const { view, tab } = parseHash();
    showView(view, tab);
  }
  window.addEventListener('hashchange', showViewFromHash);
  window.addEventListener('popstate', showViewFromHash);

  for (const link of viewLinks) {
    link.addEventListener('click', (event) => {
      if (event.metaKey || event.ctrlKey || event.shiftKey || event.button !== 0) return;
      event.preventDefault();
      const viewName = link.dataset.viewLink;
      showView(viewName);
      if (location.hash.split('/')[0] !== '#' + viewName) history.pushState(null, '', '#' + viewName);
      // Move focus to the view heading so keyboard and screen reader users land in the new view.
      const heading = document.querySelector('[data-view="' + viewName + '"] h1');
      if (heading) {
        heading.tabIndex = -1;
        heading.focus({ preventScroll: true });
      }
      window.scrollTo({ top: 0 });
    });
  }

  const initial = parseHash();
  showView(initial.view, initial.tab);

  // --- Source editor ---

  // admin.js scrolls to and focuses the source form when Change is clicked; open its tab first (capture phase runs before admin.js).
  document.addEventListener('click', (event) => {
    if (event.target.closest('.change-source')) openTab('sources', 'source-editor');
  }, true);

  // Name the editor tab after what it is doing: adding a new source or changing an existing one.
  const editorTab = document.getElementById('tab-source-editor');
  const saveButton = document.getElementById('source-save');
  const nameInput = document.getElementById('source-name');
  function syncEditorTabLabel() {
    const editing = /update/i.test(saveButton.textContent || '');
    const name = String(nameInput.value || '').trim();
    editorTab.textContent = editing ? 'Change ' + (name || 'source') : 'Add a source';
  }
  new MutationObserver(syncEditorTabLabel).observe(saveButton, { childList: true, characterData: true, subtree: true });
  syncEditorTabLabel();

  // Wide tables scroll sideways on small screens; make each scroll area reachable and named for keyboard users.
  document.querySelectorAll('.table-wrap').forEach((wrap) => {
    const caption = wrap.querySelector('caption')?.textContent?.trim();
    wrap.tabIndex = 0;
    wrap.setAttribute('role', 'region');
    if (caption) wrap.setAttribute('aria-label', caption);
  });
})();
