import type { ReactNode } from 'react'

export function PageHeading({ title, description, actions }: { title: ReactNode; description?: string; actions?: ReactNode }) {
  return <div className="mb-5 flex min-w-0 flex-col justify-between gap-3 sm:flex-row sm:items-center">
    <div className="min-w-0">
      <h1 className="min-w-0 truncate text-xl font-semibold tracking-tight md:text-2xl">{title}</h1>
      {description && <p className="mt-1 text-sm text-muted-foreground">{description}</p>}
    </div>
    {actions && <div className="flex shrink-0 flex-wrap items-center gap-2">{actions}</div>}
  </div>
}
