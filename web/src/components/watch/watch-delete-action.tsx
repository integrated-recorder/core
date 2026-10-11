import { Trash2 } from 'lucide-react'
import { Confirm } from '@/components/ui/confirm'
import { Button } from '@/components/ui/button'
import { useI18n } from '@/i18n/provider'

export function WatchDeleteAction({ onDelete, disabled }: { onDelete: () => void; disabled?: boolean }) {
  const { t } = useI18n()
  return <Confirm
    trigger={<Button variant="destructive" disabled={disabled}><Trash2 className="h-4 w-4" />{t('watch.delete')}</Button>}
    title={t('watch.delete.title')}
    description={t('watch.delete.description')}
    confirmLabel={t('watch.delete.confirm')}
    destructive
    onConfirm={onDelete}
    disabled={disabled}
  />
}
