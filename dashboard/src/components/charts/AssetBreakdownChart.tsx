import { useEffect, useRef } from 'react'
import * as echarts from 'echarts/core'
import { BarChart } from 'echarts/charts'
import { GridComponent, TooltipComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import type { AssetClassBreakdown } from '../../types'

echarts.use([BarChart, GridComponent, TooltipComponent, CanvasRenderer])

interface Props {
  data:    AssetClassBreakdown[]
  height?: number
  isDark?: boolean
}

export default function AssetBreakdownChart({ data, height = 240, isDark = true }: Props) {
  const ref   = useRef<HTMLDivElement>(null)
  const chart = useRef<echarts.ECharts | null>(null)

  useEffect(() => {
    if (!ref.current || !data.length) return

    chart.current = echarts.init(ref.current)

    const totals: Record<string, number> = {}
    const counts: Record<string, number> = {}
    for (const d of data) {
      totals[d.assetClass] = (totals[d.assetClass] ?? 0) + d.totalPnl
      counts[d.assetClass] = (counts[d.assetClass] ?? 0) + d.tradeCount
    }

    const classes = Object.keys(totals).sort((a, b) => totals[b] - totals[a])
    const values  = classes.map(c => Math.round(totals[c]))
    const colors  = values.map(v => v >= 0 ? '#3fb950' : '#f85149')

    const axisColor  = isDark ? '#8b949e' : '#656d76'
    const splitColor = isDark ? '#21262d' : '#d0d7de'

    chart.current.setOption({
      backgroundColor: 'transparent',
      tooltip: {
        trigger: 'axis',
        backgroundColor: isDark ? '#161b22' : '#ffffff',
        borderColor:     isDark ? '#30363d'  : '#d0d7de',
        textStyle:       { color: isDark ? '#e6edf3' : '#1f2328' },
        formatter: (params: any) => {
          const p   = params[0]
          const cls = p.name
          const val = p.value
          const cnt = counts[cls] ?? 0
          return `<b>${cls}</b><br/>PnL: ${val >= 0 ? '+' : ''}$${val.toLocaleString()}<br/>Trades: ${cnt}`
        },
      },
      grid: { top: 16, bottom: 36, left: 70, right: 60 },
      xAxis: {
        type: 'value',
        axisLabel: {
          color: axisColor, fontSize: 11,
          formatter: (v: number) =>
            `$${v >= 0 ? '' : '-'}${Math.abs(v / 1000).toFixed(0)}k`,
        },
        axisLine:  { lineStyle: { color: splitColor } },
        splitLine: { lineStyle: { color: splitColor } },
      },
      yAxis: {
        type: 'category',
        data: classes,
        axisLabel: {
          color: axisColor, fontSize: 12,
          formatter: (v: string) => v.charAt(0).toUpperCase() + v.slice(1),
        },
        axisLine: { lineStyle: { color: splitColor } },
        axisTick: { show: false },
      },
      series: [{
        type: 'bar',
        data: values.map((v, i) => ({
          value: v,
          itemStyle: { color: colors[i], borderRadius: [0, 4, 4, 0] },
        })),
        barMaxWidth: 40,
        label: {
          show: true,
          position: values.map(v => v >= 0 ? 'right' : 'left'),
          formatter: (p: any) => {
            const v = p.value
            return `${v >= 0 ? '+' : ''}$${(Math.abs(v) / 1000).toFixed(1)}k`
          },
          color: axisColor, fontSize: 11,
        },
      }],
    })

    const ro = new ResizeObserver(() => chart.current?.resize())
    ro.observe(ref.current)
    return () => { ro.disconnect(); chart.current?.dispose(); chart.current = null }
  }, [data, isDark])

  if (!data.length) return null

  return <div ref={ref} style={{ width: '100%', height }} />
}
