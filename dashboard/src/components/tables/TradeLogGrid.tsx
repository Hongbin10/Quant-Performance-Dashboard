import 'ag-grid-community/styles/ag-grid.css'
import 'ag-grid-community/styles/ag-theme-alpine.css'
import { AgGridReact } from 'ag-grid-react'
import { useMemo } from 'react'
import type { ColDef } from 'ag-grid-community'
import type { Trade } from '../../types'
import { useThemeMode } from '../../context/ThemeContext'

const monoStyle = () => ({ fontFamily: '"IBM Plex Mono", monospace', fontSize: '13px' })
const pnlStyle  = (p: any) => ({
  color: p.value > 0 ? '#3fb950' : p.value < 0 ? '#f85149' : '#8b949e',
  fontFamily: '"IBM Plex Mono", monospace', fontWeight: 600, fontSize: '13px',
})

interface Props {
  data:     Trade[]
  loading?: boolean
}

export default function TradeLogGrid({ data, loading }: Props) {
  const { mode } = useThemeMode()
  const agClass = mode === 'dark' ? 'ag-theme-alpine-dark' : 'ag-theme-alpine'
  const colDefs: ColDef<Trade>[] = useMemo(() => [
    { field: 'tradeDate',  headerName: 'Date',    width: 115, pinned: 'left', cellStyle: monoStyle },
    { field: 'strategy',   headerName: 'Strategy', minWidth: 140, flex: 1.8 },
    { field: 'symbol',     headerName: 'Symbol',   minWidth: 80,  flex: 0.9, cellStyle: monoStyle },
    { field: 'assetClass', headerName: 'Class',    minWidth: 85,  flex: 1,
      cellStyle: () => ({ color: '#58a6ff', fontFamily: '"IBM Plex Mono", monospace', fontSize: '13px' }) },
    { field: 'direction',  headerName: 'Dir',      minWidth: 70,  flex: 0.8,
      cellStyle: (p) => ({ color: p.value === 'Long' ? '#3fb950' : '#f85149', fontWeight: 600, fontSize: '13px' }) },
    { field: 'entryPrice', headerName: 'Entry',    minWidth: 100, flex: 1,
      valueFormatter: (p) => p.value != null ? p.value.toFixed(4) : '', cellStyle: monoStyle },
    { field: 'exitPrice',  headerName: 'Exit',     minWidth: 100, flex: 1,
      valueFormatter: (p) => p.value != null ? p.value.toFixed(4) : '', cellStyle: monoStyle },
    { field: 'quantity',   headerName: 'Qty',      minWidth: 70,  flex: 0.8, cellStyle: monoStyle },
    { field: 'pnl',        headerName: 'PnL ($)',  minWidth: 115, flex: 1.3,
      valueFormatter: (p) => p.value != null
        ? `$${Number(p.value).toLocaleString('en-US', { minimumFractionDigits: 2 })}`
        : '$0.00',
      cellStyle: pnlStyle, sort: 'desc' },
    { field: 'returnPct',  headerName: 'Ret%',     minWidth: 85,  flex: 1,
      valueFormatter: (p) => p.value != null ? `${Number(p.value).toFixed(3)}%` : '',
      cellStyle: pnlStyle },
  ], [])

  const defaultColDef: ColDef = useMemo(() => ({
    resizable: true, sortable: true, filter: true,
  }), [])

  return (
    <div className={agClass} style={{ width: '100%', height: 600 }}>
      <AgGridReact
        rowData={data}
        columnDefs={colDefs}
        defaultColDef={defaultColDef}
        loading={loading}
        animateRows
        suppressCellFocus
        rowHeight={44}
        headerHeight={48}
        pagination
        paginationPageSize={50}
        paginationPageSizeSelector={[20, 50, 100, 200]}
      />
    </div>
  )
}