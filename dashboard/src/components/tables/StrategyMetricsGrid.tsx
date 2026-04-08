import { AgGridReact } from 'ag-grid-react'
import { useNavigate } from 'react-router-dom'
import { useMemo } from 'react'
import type { ColDef, ICellRendererParams } from 'ag-grid-community'
import SparklineCellRenderer from './SparklineCellRenderer'
import type { StrategyMetrics } from '../../types'
import { useThemeMode } from '../../context/ThemeContext'

const fmtCurrency = (p: any) =>
  p.value != null
    ? `$${Number(p.value).toLocaleString('en-US', { minimumFractionDigits: 2 })}`
    : '—'

const fmtPct = (p: any) =>
  p.value != null ? `${Number(p.value).toFixed(2)}%` : '—'

const fmtNum = (p: any) =>
  p.value != null ? Number(p.value).toFixed(2) : '—'

const mono = () => ({ fontFamily: '"IBM Plex Mono", monospace', fontSize: '13px' })

const pnlStyle = (p: any) => ({
  color: p.value > 0 ? '#3fb950' : p.value < 0 ? '#f85149' : '#8b949e',
  fontFamily: '"IBM Plex Mono", monospace', fontSize: '13px', fontWeight: 600,
})

const sharpeStyle = (p: any) => ({
  color: p.value >= 1.5 ? '#3fb950' : p.value >= 0.5 ? '#d29922' : '#f85149',
  fontFamily: '"IBM Plex Mono", monospace', fontSize: '13px', fontWeight: 600,
})

const ddStyle = (p: any) => ({
  color: p.value > 20 ? '#f85149' : p.value > 10 ? '#d29922' : '#3fb950',
  fontFamily: '"IBM Plex Mono", monospace', fontSize: '13px',
})

function ActionsCellRenderer(params: ICellRendererParams) {
  const navigate = useNavigate()
  const id = params.data?.strategyId

  return (
    <div style={{ display: 'flex', alignItems: 'center', height: '100%' }}>
      <button
        onClick={() => navigate(`/strategies/${id}`)}
        style={{
          background: 'transparent',
          border: '0.5px solid rgba(88,166,255,0.35)',
          borderRadius: 4,
          color: '#58a6ff',
          cursor: 'pointer',
          fontSize: 11,
          fontFamily: '"IBM Plex Mono", monospace',
          padding: '2px 8px',
          whiteSpace: 'nowrap',
          transition: 'background 0.15s',
        }}
        onMouseEnter={(e) => (e.currentTarget.style.background = 'rgba(88,166,255,0.1)')}
        onMouseLeave={(e) => (e.currentTarget.style.background = 'transparent')}
      >
        View Detail →
      </button>
    </div>
  )
}

interface Props {
  data:     StrategyMetrics[]
  loading?: boolean
}

export default function StrategyMetricsGrid({ data, loading }: Props) {
  const { mode } = useThemeMode()
  const agClass = mode === 'dark' ? 'ag-theme-balham-dark' : 'ag-theme-balham'
  const colDefs: ColDef<StrategyMetrics>[] = useMemo(() => [
    {
      field: 'strategyName',
      headerName: 'Strategy',
      pinned: 'left',
      minWidth: 145,
      flex: 1.4,
    },
    {
      field: 'totalPnl',
      headerName: 'Total PnL',
      minWidth: 120,
      flex: 1.3,
      valueFormatter: fmtCurrency,
      cellStyle: pnlStyle,
      sort: 'desc',
      filter: 'agNumberColumnFilter',
    },
    {
      field: 'equityHistory',
      headerName: 'Equity Curve',
      minWidth: 120,
      flex: 1.4,
      sortable: false,
      filter: false,
      cellRenderer: SparklineCellRenderer,
      valueFormatter: () => '',
    },
    {
      field: 'sharpeRatio',
      headerName: 'Sharpe',
      minWidth: 85,
      flex: 0.9,
      valueFormatter: fmtNum,
      cellStyle: sharpeStyle,
      filter: 'agNumberColumnFilter',
    },
    {
      field: 'maxDrawdownPct',
      headerName: 'Max DD',
      minWidth: 85,
      flex: 0.9,
      valueFormatter: fmtPct,
      cellStyle: ddStyle,
      filter: 'agNumberColumnFilter',
    },
    {
      field: 'winRatePct',
      headerName: 'Win Rate',
      minWidth: 85,
      flex: 0.9,
      valueFormatter: fmtPct,
      cellStyle: mono,
      filter: 'agNumberColumnFilter',
    },
    {
      field: 'totalTrades',
      headerName: 'Trades',
      minWidth: 75,
      flex: 0.8,
      cellStyle: mono,
      filter: 'agNumberColumnFilter',
    },
    {
      headerName: '',
      field: 'strategyId',
      width: 120,
      minWidth: 120,
      maxWidth: 120,
      sortable: false,
      filter: false,
      resizable: false,
      cellRenderer: ActionsCellRenderer,
      valueFormatter: () => '',
    },
  ], [])

  const defaultColDef: ColDef = useMemo(() => ({
    resizable: true,
    sortable: true,
  }), [])

  return (
    <div className={agClass} style={{ width: '100%', height: 300 }}>
      <AgGridReact
        domLayout="autoHeight"
        rowData={data ?? []}
        columnDefs={colDefs}
        defaultColDef={defaultColDef}
        loading={loading}
        suppressCellFocus
        animateRows
        rowHeight={44}
        headerHeight={36}
      />
    </div>
  )
}
