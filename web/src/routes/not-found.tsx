import { Link } from '@tanstack/react-router'
import { FileQuestion } from 'lucide-react'
import { Button } from '@/components/ui/button'
import { useI18n } from '@/i18n/provider'
export function NotFoundPage() { const { t } = useI18n(); return <div className="mx-auto grid min-h-[65vh] max-w-md place-items-center text-center"><div><div className="mx-auto grid h-14 w-14 place-items-center rounded-2xl bg-muted"><FileQuestion className="h-7 w-7 text-muted-foreground" /></div><h1 className="mt-5 text-xl font-semibold">{t('errors.notFound')}</h1><Link to="/"><Button className="mt-5">{t('nav.dashboard')}</Button></Link></div></div> }
