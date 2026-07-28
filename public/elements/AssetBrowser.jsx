// AssetBrowser — two-pane asset browser rendered inside cl.ElementSidebar.
// No JSX — React, render, and props are injected by Chainlit's react-live runner.

// ── Helpers ──────────────────────────────────────────────────────────────────

const TYPE_EMOJI = {
  echart: '📊', chart: '📊',
  table: '📋', csv: '📋',
  report: '📄', html: '🌐',
  image: '🖼', pdf: '📄',
  json: '📁', js: '📁',
};

function fmtDate(ts) {
  if (!ts) return '';
  try {
    return new Date(ts * 1000).toLocaleString('en-US', {
      month: 'short', day: 'numeric',
      hour: '2-digit', minute: '2-digit',
    });
  } catch (_) { return ''; }
}

// ── Sub-components ────────────────────────────────────────────────────────────

function ChartPreview({ asset }) {
  const ref   = React.useRef(null);
  const [err, setErr] = React.useState('');

  React.useEffect(() => {
    let chart = null;

    function init() {
      if (!ref.current) return;
      try {
        const opt = JSON.parse(asset.content);
        delete opt._height;
        delete opt._description;
        chart = window.echarts.init(ref.current, null, { renderer: 'canvas' });
        chart.setOption(opt);
      } catch (e) {
        setErr(String(e));
      }
    }

    if (window.echarts) {
      init();
    } else {
      // Lazy-load only once — subsequent charts reuse the cached global.
      if (!document.getElementById('__echarts_cdn__')) {
        const s = document.createElement('script');
        s.id  = '__echarts_cdn__';
        s.src = 'https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js';
        s.onload  = init;
        s.onerror = () => setErr('Failed to load ECharts from CDN');
        document.head.appendChild(s);
      } else {
        // Script tag exists but echarts not yet available — poll briefly
        const iv = setInterval(() => {
          if (window.echarts) { clearInterval(iv); init(); }
        }, 100);
        return () => clearInterval(iv);
      }
    }

    return () => { if (chart) chart.dispose(); };
  }, [asset.id]);

  if (err) return React.createElement('div', {
    style: { color: '#ef4444', fontSize: 12, padding: 8 }
  }, 'Chart error: ' + err);

  return React.createElement('div', {
    ref,
    style: { width: '100%', height: 280 },
  });
}

function TablePreview({ asset }) {
  let data;
  try { data = JSON.parse(asset.content); }
  catch (_) {
    return React.createElement('pre', {
      style: { fontSize: 11, whiteSpace: 'pre-wrap', color: '#374151' }
    }, asset.content.slice(0, 1000));
  }

  const rawCols = data.columns || [];
  const cols = rawCols.map(c =>
    typeof c === 'object' ? (c.name || c.label || JSON.stringify(c)) : String(c)
  );
  const rows = (data.rows || []).slice(0, 30);

  if (!cols.length) return React.createElement('div', {
    style: { color: '#9ca3af', fontSize: 12 }
  }, 'Empty table');

  return React.createElement('div', { style: { overflowX: 'auto' } },
    React.createElement('table', {
      style: { borderCollapse: 'collapse', width: '100%', fontSize: 12 }
    },
      React.createElement('thead', null,
        React.createElement('tr', null,
          cols.map((col, i) =>
            React.createElement('th', {
              key: i,
              style: {
                padding: '5px 8px', textAlign: 'left', fontWeight: 600,
                background: '#f3f4f6', borderBottom: '1px solid #e5e7eb',
                whiteSpace: 'nowrap',
              }
            }, col)
          )
        )
      ),
      React.createElement('tbody', null,
        rows.map((row, ri) =>
          React.createElement('tr', {
            key: ri,
            style: { background: ri % 2 === 0 ? '#fff' : '#fafafa' }
          },
            cols.map((col, ci) => {
              const val = Array.isArray(row)
                ? row[ci]
                : (row[col] ?? row[Object.keys(row)[ci]] ?? '');
              return React.createElement('td', {
                key: ci,
                style: {
                  padding: '4px 8px',
                  borderBottom: '1px solid #f3f4f6',
                  maxWidth: 200,
                  overflow: 'hidden',
                  textOverflow: 'ellipsis',
                  whiteSpace: 'nowrap',
                }
              }, String(val ?? ''));
            })
          )
        )
      )
    ),
    rows.length === 30 && React.createElement('div', {
      style: { color: '#9ca3af', fontSize: 11, marginTop: 4, textAlign: 'right' }
    }, 'Showing first 30 rows')
  );
}

function ImagePreview({ asset }) {
  const src = 'data:' + (asset.mime_type || 'image/png') + ';base64,' + asset.content;
  return React.createElement('img', {
    src,
    style: {
      maxWidth: '100%', maxHeight: 320,
      objectFit: 'contain', borderRadius: 4,
      border: '1px solid #e5e7eb',
    },
  });
}

function TextPreview({ asset }) {
  const text = (asset.content || '').slice(0, 4000);
  return React.createElement('pre', {
    style: {
      fontSize: 11, whiteSpace: 'pre-wrap', lineHeight: 1.6,
      color: '#374151', margin: 0,
      maxHeight: 400, overflowY: 'auto',
    }
  }, text);
}

function PreviewPane({ asset }) {
  if (!asset) return React.createElement('div', {
    style: {
      display: 'flex', flexDirection: 'column', alignItems: 'center',
      justifyContent: 'center', height: '100%', color: '#9ca3af',
    }
  },
    React.createElement('div', { style: { fontSize: 32, marginBottom: 8 } }, '📂'),
    React.createElement('div', { style: { fontSize: 13 } }, 'Select an asset to preview')
  );

  const atype = asset.asset_type || '';
  let body;
  if ((atype === 'echart' || atype === 'chart') && asset.content) {
    body = React.createElement(ChartPreview, { asset, key: asset.id });
  } else if ((atype === 'table' || atype === 'csv') && asset.content) {
    body = React.createElement(TablePreview, { asset, key: asset.id });
  } else if (atype === 'image' && asset.content) {
    body = React.createElement(ImagePreview, { asset, key: asset.id });
  } else if (atype === 'pdf') {
    body = React.createElement('div', {
      style: {
        background: '#f9fafb', border: '1px solid #e5e7eb', borderRadius: 6,
        padding: '24px 16px', textAlign: 'center', color: '#6b7280', fontSize: 13,
      }
    },
      React.createElement('div', { style: { fontSize: 28, marginBottom: 8 } }, '📄'),
      React.createElement('div', null, 'PDF preview not available inline.'),
      React.createElement('div', { style: { fontSize: 11, marginTop: 4, color: '#9ca3af' } },
        'Ask Corax to display this file in the chat.'
      )
    );
  } else if (asset.content) {
    body = React.createElement(TextPreview, { asset, key: asset.id });
  } else {
    body = React.createElement('div', {
      style: { color: '#9ca3af', fontSize: 12 }
    }, 'No preview available');
  }

  return React.createElement('div', { style: { height: '100%', overflowY: 'auto' } },
    // Header
    React.createElement('div', { style: { marginBottom: 12 } },
      React.createElement('div', {
        style: { fontWeight: 600, fontSize: 14, color: '#111827', marginBottom: 2 }
      }, (TYPE_EMOJI[atype] || '📁') + ' ' + (asset.title || 'Untitled')),
      React.createElement('div', { style: { fontSize: 11, color: '#9ca3af' } },
        [
          atype,
          asset.org,
          fmtDate(asset.created_at),
        ].filter(Boolean).join(' · ')
      )
    ),
    // Divider
    React.createElement('hr', { style: { border: 'none', borderTop: '1px solid #f3f4f6', margin: '8px 0 12px' } }),
    // Content
    body
  );
}

// ── Main component ────────────────────────────────────────────────────────────

const AssetBrowser = () => {
  const assets = (props.assets || []);
  const [selectedId, setSelectedId] = React.useState(
    assets.length > 0 ? assets[0].id : null
  );
  const selected = assets.find(a => a.id === selectedId) || null;

  return React.createElement('div', {
    style: {
      display: 'flex',
      height: 'calc(100vh - 80px)',
      minHeight: 400,
      fontFamily: 'system-ui, -apple-system, sans-serif',
      fontSize: 13,
      overflow: 'hidden',
    }
  },

    // ── Left: asset list ──────────────────────────────────────────────────
    React.createElement('div', {
      style: {
        width: 190,
        minWidth: 150,
        borderRight: '1px solid #e5e7eb',
        overflowY: 'auto',
        flexShrink: 0,
        background: '#f9fafb',
      }
    },
      assets.length === 0
        ? React.createElement('div', {
            style: { color: '#9ca3af', padding: '16px 12px', fontSize: 12 }
          }, 'No assets saved yet.')
        : assets.map(asset =>
            React.createElement('div', {
              key: asset.id,
              onClick: () => setSelectedId(asset.id),
              style: {
                padding: '9px 10px',
                cursor: 'pointer',
                borderBottom: '1px solid #f0f0f0',
                borderLeft: asset.id === selectedId
                  ? '3px solid #3b82f6'
                  : '3px solid transparent',
                background: asset.id === selectedId ? '#eff6ff' : 'transparent',
                transition: 'background 0.1s',
              }
            },
              React.createElement('div', {
                style: {
                  fontWeight: 500, fontSize: 12,
                  color: asset.id === selectedId ? '#1d4ed8' : '#1f2937',
                  marginBottom: 2,
                  whiteSpace: 'nowrap', overflow: 'hidden', textOverflow: 'ellipsis',
                }
              }, (TYPE_EMOJI[asset.asset_type] || '📁') + ' ' + (asset.title || 'Untitled')),
              React.createElement('div', {
                style: { fontSize: 10, color: '#9ca3af' }
              }, fmtDate(asset.created_at))
            )
          )
    ),

    // ── Right: preview pane ───────────────────────────────────────────────
    React.createElement('div', {
      style: {
        flex: 1,
        padding: '14px 16px',
        overflowY: 'auto',
        minWidth: 0,
      }
    },
      React.createElement(PreviewPane, { asset: selected })
    )
  );
};

render(React.createElement(AssetBrowser));
