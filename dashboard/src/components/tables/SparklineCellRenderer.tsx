import { useEffect, useRef } from 'react'
import * as echarts from 'echarts/core'
import { LineChart } from 'echarts/charts'
import { GridComponent } from 'echarts/components'
import { CanvasRenderer } from 'echarts/renderers'
import type { ICellRendererParams } from 'ag-grid-community'

echarts.use([LineChart, GridComponent, CanvasRenderer])

export default function SparklineCellRenderer(params: ICellRendererParams) {
  const ref   = useRef<HTMLDivElement>(null)
  const chart = useRef<echarts.ECharts | null>(null)
  const value: number[] = params.value ?? []

  useEffect(() => {
    if (!ref.current || value.length === 0) return

    const timer = setTimeout(() => {
      if (!ref.current) return

      chart.current = echarts.init(ref.current)

      const last  = value[value.length - 1]
      const first = value[0]
      const isUp  = last >= first
      const color = isUp ? '#3fb950' : '#f85149'

      chart.current.setOption({
        animation: false,
        grid: { top: 2, bottom: 2, left: 2, right: 2 },
        xAxis: { type: 'category', show: false },
        yAxis: { type: 'value',    show: false, scale: true },
        series: [{
          type: 'line',
          data: value,
          smooth: true,
          symbol: 'none',
          lineStyle: { color, width: 1.5 },
          areaStyle: {
            color: {
              type: 'linear', x: 0, y: 0, x2: 0, y2: 1,
              colorStops: [
                { offset: 0, color: color + '55' },
                { offset: 1, color: color + '00' },
              ],
            },
          },
        }],
      })

      // Watch container size — resize chart when column is dragged
      const ro = new ResizeObserver(() => {
        chart.current?.resize()
      })
      ro.observe(ref.current)

      // Store cleanup on the div for later
      ;(ref.current as any).__ro = ro
    }, 0)

    return () => {
      clearTimeout(timer)
      ;(ref.current as any)?.__ro?.disconnect()
      chart.current?.dispose()
      chart.current = null
    }
  }, [JSON.stringify(value)])

  if (value.length === 0) {
    return <span style={{ color: '#8b949e', fontSize: 11 }}>—</span>
  }

  return <div ref={ref} style={{ width: '100%', height: '100%', minHeight: 32 }} />
}
