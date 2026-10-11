import { cloneElement, isValidElement, useEffect, useId, useRef, useState, type HTMLAttributes, type MouseEvent, type ReactElement, type ReactNode } from 'react'
import { Button } from './button'
import { useI18n } from '@/i18n/provider'

export function Confirm({ trigger, title, description, confirmLabel, destructive = false, onConfirm, disabled }: { trigger: ReactNode; title: string; description: string; confirmLabel?: string; destructive?: boolean; onConfirm: () => void; disabled?: boolean }) {
  const { t } = useI18n()
  const [open, setOpen] = useState(false)
  const titleId = useId()
  const descriptionId = useId()
  const cancelRef = useRef<HTMLButtonElement>(null)
  const returnFocus = useRef<HTMLElement | null>(null)

  useEffect(() => {
    if (!open) return
    cancelRef.current?.focus()
    const previous = returnFocus.current
    return () => previous?.focus()
  }, [open])

  const close = () => setOpen(false)
  const triggerNode = isValidElement(trigger)
    ? cloneElement(trigger as ReactElement<HTMLAttributes<HTMLElement>>, {
      'aria-haspopup': 'dialog',
      'aria-expanded': open,
      onClick: event => {
        (trigger.props as { onClick?: (event: MouseEvent<HTMLElement>) => void }).onClick?.(event)
        if (!event.defaultPrevented) { returnFocus.current = event.currentTarget; setOpen(true) }
      },
    })
    : <span onClick={event => { returnFocus.current = event.currentTarget; setOpen(true) }}>{trigger}</span>

  return <>
    {triggerNode}
    {open && <div className="fixed inset-0 z-50 grid place-items-center overflow-y-auto bg-black/45 p-4 backdrop-blur-[2px]" onMouseDown={event => event.stopPropagation()}>
      <section role="alertdialog" aria-modal="true" aria-labelledby={titleId} aria-describedby={descriptionId} className="w-full max-w-md rounded-xl border border-border bg-card p-6 text-card-foreground shadow-xl focus:outline-none" onKeyDown={event => {
        if (event.key === 'Escape') { event.preventDefault(); close(); return }
        if (event.key === 'Tab') {
          const buttons = event.currentTarget.querySelectorAll<HTMLButtonElement>('button:not(:disabled)')
          const first = buttons[0]; const last = buttons[buttons.length - 1]
          if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus() }
          else if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus() }
        }
      }}>
        <h2 id={titleId} className="pr-8 text-lg font-semibold">{title}</h2>
        <p id={descriptionId} className="mt-2 text-sm leading-6 text-muted-foreground">{description}</p>
        <div className="mt-6 flex justify-end gap-2"><Button ref={cancelRef} variant="outline" onClick={close}>{t('common.cancel')}</Button><Button variant={destructive ? 'destructive' : 'default'} disabled={disabled} onClick={() => { onConfirm(); close() }}>{confirmLabel ?? t('common.confirm')}</Button></div>
      </section>
    </div>}
  </>
}
