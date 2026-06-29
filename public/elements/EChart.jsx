// No JSX — avoids Chainlit's sucrase production/jsx-runtime issue.
// React, render, and props are injected into scope by Chainlit's react-live runner.
const EChartElement = () => {
  const containerRef = React.useRef(null);
  const [ready, setReady] = React.useState(false);
  const [error, setError] = React.useState('');

  React.useEffect(() => {
    if (window.echarts) {
      setReady(true);
      return;
    }
    const s = document.createElement('script');
    s.src = 'https://cdn.jsdelivr.net/npm/echarts@5/dist/echarts.min.js';
    s.onload = () => setReady(true);
    s.onerror = () => setError('Failed to load ECharts from CDN');
    document.head.appendChild(s);
  }, []);

  React.useEffect(() => {
    if (!ready || !containerRef.current) return;
    try {
      const option = Object.assign({}, props.option);
      delete option._height;
      delete option._description;
      const chart = window.echarts.init(containerRef.current, null, { renderer: 'canvas' });
      chart.setOption(option);
      const onResize = () => chart.resize();
      window.addEventListener('resize', onResize);
      return () => {
        window.removeEventListener('resize', onResize);
        chart.dispose();
      };
    } catch (e) {
      setError(String(e));
    }
  }, [ready]);

  const height = (props.option && props.option._height) || props.height || 320;
  const desc   = (props.option && props.option._description) || '';

  if (error) {
    return React.createElement('div', {
      style: { color: '#ef4444', padding: '8px', fontSize: '0.8rem' }
    }, 'Chart error: ' + error);
  }

  return React.createElement('div', { style: { width: '100%' } },
    React.createElement('div', {
      ref: containerRef,
      style: { width: '100%', height: height + 'px' },
    }),
    !ready && React.createElement('div', {
      style: { textAlign: 'center', color: '#9ca3af', fontSize: '0.8rem', marginTop: '8px' }
    }, 'Loading chart…'),
    desc && React.createElement('p', {
      style: { fontSize: '0.8rem', color: '#6b7280', marginTop: '4px', fontStyle: 'italic' }
    }, desc),
  );
};

render(React.createElement(EChartElement));
