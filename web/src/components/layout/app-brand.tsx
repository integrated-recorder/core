import { Archive } from 'lucide-react'
import { cn } from '@/lib/utils'

type AppBrandProps = { variant?: 'sidebar' | 'mobile' | 'login' | 'compact'; className?: string; inverse?: boolean }

export function AppBrand({ variant = 'sidebar', className, inverse = false }: AppBrandProps) {
  const compact = variant === 'compact'
  const markSize = variant === 'login' ? 'h-11 w-11 rounded-xl' : compact ? 'h-8 w-8 rounded-md' : 'h-9 w-9 rounded-lg'
  const iconSize = variant === 'login' ? 'h-6 w-6' : compact ? 'h-4 w-4' : 'h-5 w-5'
  return <div className={cn('flex min-w-0 items-center gap-3', className)} aria-label="Integrated Recorder">
    <span className={cn('grid shrink-0 aspect-square place-items-center bg-primary text-primary-foreground', markSize, inverse && 'bg-blue-500')} aria-hidden="true"><Archive className={cn('shrink-0', iconSize)} /></span>
    <span className="min-w-0">
      <span className={cn('block whitespace-nowrap font-bold tracking-tight', compact ? 'text-xs' : variant === 'login' ? 'text-base' : 'text-sm', inverse && 'text-white')}>Integrated Recorder</span>
    </span>
  </div>
}
