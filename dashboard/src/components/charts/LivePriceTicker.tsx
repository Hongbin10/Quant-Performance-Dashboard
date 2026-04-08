import { Box, Card, CardContent, Chip, Grid, Typography } from '@mui/material'
import { useEffect, useRef, useState } from 'react'
import { useMarketData } from '../../hooks/useMarketData'

// ── Two groups mirroring QRT's trading universe ───────────────────────────────
const MARKET_OVERVIEW = [
  'BINANCE:BTCUSDT',
  'BINANCE:ETHUSDT',
  'OANDA:USD_JPY',
  'OANDA:BCO_USD',
  'OANDA:XAU_USD',
  'OANDA:EUR_USD',
]

const EQUITIES = [
  'AAPL', 'MSFT', 'NVDA', 'AMZN', 'META', 'GOOGL',
]

const ALL_SYMBOLS = [...MARKET_OVERVIEW, ...EQUITIES]

const DISPLAY_NAMES: Record<string, string> = {
  'BINANCE:BTCUSDT': 'BTC/USD',
  'BINANCE:ETHUSDT': 'ETH/USD',
  'OANDA:USD_JPY':   'USD/JPY',
  'OANDA:BCO_USD':   'Brent Oil',
  'OANDA:XAU_USD':   'Gold',
  'OANDA:EUR_USD':   'EUR/USD',
  'AAPL': 'AAPL', 'MSFT': 'MSFT', 'NVDA': 'NVDA',
  'AMZN': 'AMZN', 'META': 'META', 'GOOGL': 'GOOGL',
}

// Decimal places per symbol
const DECIMALS: Record<string, number> = {
  'BINANCE:BTCUSDT': 0,
  'BINANCE:ETHUSDT': 1,
  'OANDA:BCO_USD':   2,
  'OANDA:XAU_USD':   1,
  'OANDA:EUR_USD':   4,
  'OANDA:GBP_USD':   4,
}
const getDecimals = (sym: string) => DECIMALS[sym] ?? 2

// ── Single price card ─────────────────────────────────────────────────────────
interface PriceCardProps {
  symbol: string
  price:  number | undefined
}

function PriceCard({ symbol, price }: PriceCardProps) {
  const prevRef = useRef<number | undefined>(undefined)
  const openRef = useRef<number | undefined>(undefined)
  const [flash, setFlash] = useState<'up' | 'down' | null>(null)

  useEffect(() => {
    if (price === undefined) return
    if (openRef.current === undefined) openRef.current = price
    if (prevRef.current !== undefined && price !== prevRef.current) {
      setFlash(price > prevRef.current ? 'up' : 'down')
      const t = setTimeout(() => setFlash(null), 500)
      prevRef.current = price
      return () => clearTimeout(t)
    }
    prevRef.current = price
  }, [price])

  const open      = openRef.current
  const dec       = getDecimals(symbol)
  const change    = price !== undefined && open !== undefined ? price - open : undefined
  const changePct = change !== undefined && open ? (change / open) * 100 : undefined
  const isUp      = change !== undefined && change >= 0
  const changeColor = change === undefined ? '#8b949e' : isUp ? '#3fb950' : '#f85149'

  const bgColor =
    flash === 'up'   ? 'rgba(63,185,80,0.12)'  :
    flash === 'down' ? 'rgba(248,81,73,0.12)'   :
    'background.paper'

  return (
    <Card sx={{ height: '100%', transition: 'background 0.25s', bgcolor: bgColor }}>
      <CardContent sx={{ p: '10px 14px !important' }}>
        {/* Symbol label */}
        <Typography sx={{
          fontFamily: '"IBM Plex Mono", monospace', fontSize: '13px',
          color: 'text.secondary', letterSpacing: '0.04em', mb: 0.5,
        }}>
          {DISPLAY_NAMES[symbol] ?? symbol}
        </Typography>

        {/* Price */}
        <Typography sx={{
          fontFamily: '"IBM Plex Mono", monospace',
          fontSize: '1.05rem', fontWeight: 600,
          color: flash ? changeColor : 'text.primary',
          transition: 'color 0.25s', lineHeight: 1.2,
        }}>
          {price !== undefined ? price.toFixed(dec) : '—'}
        </Typography>

        {/* Change row */}
        <Box sx={{ display: 'flex', alignItems: 'center', gap: 0.5, mt: 0.5 }}>
          {change !== undefined ? (
            <>
              <Typography sx={{ fontSize: '10px', color: changeColor, lineHeight: 1 }}>
                {isUp ? '▲' : '▼'}
              </Typography>
              <Typography sx={{
                fontFamily: '"IBM Plex Mono", monospace', fontSize: '11px',
                color: changeColor, fontWeight: 500,
              }}>
                {Math.abs(changePct!).toFixed(2)}%
              </Typography>
              <Typography sx={{
                fontFamily: '"IBM Plex Mono", monospace', fontSize: '10px',
                color: 'text.secondary',
              }}>
                ({isUp ? '+' : ''}{change.toFixed(dec)})
              </Typography>
            </>
          ) : (
            <Typography sx={{ fontSize: '10px', color: 'text.secondary' }}>
              awaiting tick
            </Typography>
          )}
        </Box>
      </CardContent>
    </Card>
  )
}

// ── Group label ────────────────────────────────────────────────────────────────
function GroupLabel({ text }: { text: string }) {
  return (
    <Typography sx={{
      fontSize: '12px', fontWeight: 500, color: 'text.secondary',
      textTransform: 'uppercase', letterSpacing: '0.08em',
      mb: 1, mt: 0,
    }}>
      {text}
    </Typography>
  )
}

// ── Main component ─────────────────────────────────────────────────────────────
export default function LivePriceTicker() {
  const { prices, connected } = useMarketData(ALL_SYMBOLS)

  return (
    <Box sx={{ mb: 3 }}>
      {/* Header */}
      <Box sx={{ display: 'flex', alignItems: 'center', gap: 1.5, mb: 2 }}>
        <Typography variant="h3">Live Prices</Typography>
        <Chip
          size="small"
          label={connected ? 'Live' : 'Connecting…'}
          sx={{
            fontFamily: '"IBM Plex Mono", monospace', fontSize: '0.7rem',
            bgcolor: connected ? 'rgba(63,185,80,0.12)' : 'rgba(139,148,158,0.12)',
            color:   connected ? '#3fb950' : '#8b949e',
            border:  `1px solid ${connected ? 'rgba(63,185,80,0.3)' : 'rgba(139,148,158,0.3)'}`,
          }}
        />
        {connected && (
          <Typography sx={{
            ml: 'auto', fontSize: '13px',
            fontFamily: '"IBM Plex Mono", monospace', color: 'text.secondary',
          }}>
            US Stocks & Major FX: Near real-time (~50-200ms)
          </Typography>
        )}
      </Box>

      {/* Group 1 — Market Overview */}
      <GroupLabel text="Market overview" />
      <Grid container spacing={1.5} sx={{ mb: 2 }}>
        {MARKET_OVERVIEW.map((sym) => (
          <Grid item xs={6} sm={4} md={2} key={sym}>
            <PriceCard symbol={sym} price={prices[sym]?.price} />
          </Grid>
        ))}
      </Grid>

      {/* Group 2 — Equities */}
      <GroupLabel text="Equities" />
      <Grid container spacing={1.5}>
        {EQUITIES.map((sym) => (
          <Grid item xs={6} sm={4} md={2} key={sym}>
            <PriceCard symbol={sym} price={prices[sym]?.price} />
          </Grid>
        ))}
      </Grid>
    </Box>
  )
}
