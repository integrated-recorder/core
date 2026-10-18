import { useEffect, useState } from 'react'
import { useMutation, useQuery, useQueryClient } from '@tanstack/react-query'
import { LockKeyhole, Save } from 'lucide-react'
import { runtimeHTTPSAPI } from '@/api'
import { errorMessage } from '@/lib/errors'
import { useI18n } from '@/i18n/provider'
import type { RuntimeHTTPSSettings } from '@/types/api'
import { Button } from '@/components/ui/button'
import { Card, CardContent, CardHeader, CardTitle } from '@/components/ui/card'
import { Input } from '@/components/ui/input'
import { LoadingState, ErrorState } from '@/components/query-state'

export function HTTPSSettings() {
  const { t } = useI18n()
  const client = useQueryClient()
  const status = useQuery({ queryKey: ['runtime', 'https'], queryFn: runtimeHTTPSAPI.status })
  const [hosts, setHosts] = useState('')
  const [redirectHTTP, setRedirectHTTP] = useState(true)
  const [useACME, setUseACME] = useState(false)
  const [certificate, setCertificate] = useState('')
  const [privateKey, setPrivateKey] = useState('')
  const save = useMutation({
    mutationFn: () => useACME
      ? runtimeHTTPSAPI.enableACME({ hosts: parseHosts(hosts), redirect_http: redirectHTTP, acme: true })
      : certificate || privateKey
      ? runtimeHTTPSAPI.enableCertificate({ hosts: parseHosts(hosts), redirect_http: redirectHTTP, certificate_pem: certificate, private_key_pem: privateKey })
      : runtimeHTTPSAPI.enableSelfSigned({ hosts: parseHosts(hosts), redirect_http: redirectHTTP }),
    onSuccess: value => { client.setQueryData(['runtime', 'https'], value); setCertificate(''); setPrivateKey('') },
  })
  const disable = useMutation({ mutationFn: runtimeHTTPSAPI.disable, onSuccess: value => {
    client.setQueryData(['runtime', 'https'], value)
    if (window.location.protocol === 'https:' && value.http_recovery_available && value.http_port > 0) {
      const target = new URL(window.location.href)
      target.protocol = 'http:'
      target.port = String(value.http_port)
      window.location.replace(target.toString())
    }
  } })
  const enableSaved = useMutation({ mutationFn: runtimeHTTPSAPI.enableSaved, onSuccess: value => {
    client.setQueryData(['runtime', 'https'], value)
    setCertificate('')
    setPrivateKey('')
    if (value.certificate_ready && window.location.protocol === 'http:') {
      const target = new URL(window.location.href)
      target.protocol = 'https:'
      target.port = String(value.port)
      window.location.replace(target.toString())
    }
  } })
  const value = status.data as RuntimeHTTPSSettings | undefined
  useEffect(() => {
    if (!value?.configured) return
    setHosts(value.hosts.join(', '))
    setRedirectHTTP(value.redirect_http)
    setUseACME(value.acme)
  }, [value?.configured, value?.hosts, value?.redirect_http, value?.acme])
  return <Card className="xl:col-span-2">
    <CardHeader><CardTitle className="flex items-center gap-2"><LockKeyhole className="h-4 w-4" />{t('https.title')}</CardTitle></CardHeader>
    <CardContent className="space-y-4">
      <p className="text-sm leading-6 text-muted-foreground">{t('https.help')}</p>
      {status.isLoading ? <LoadingState /> : status.error || !value ? <ErrorState message={status.error ? errorMessage(status.error) : t('runtime.statusUnavailable')} retry={() => void status.refetch()} /> : <>
        <p role="status" className="text-sm">{!value.enabled ? t('https.disabled') : !value.listening ? t('https.listenerUnavailable') : value.issuance_failed ? t('https.issuanceFailed') : !value.certificate_ready ? t('https.pending') : t('https.enabled')} · :{value.port} · {value.redirect_active ? t('https.redirectActive') : t('https.redirectInactive')}{value.expires_at ? ` · ${t('https.expires')}: ${value.expires_at}` : ''}</p>
        {value.hosts.length > 0 && <p className="break-all text-xs text-muted-foreground">{t('https.hostsCurrent')}: {value.hosts.join(', ')}</p>}
        {value.configured && !value.enabled && <Button type="button" disabled={enableSaved.isPending} onClick={() => enableSaved.mutate()}>{t('https.enableSaved')}</Button>}
        <form className="grid gap-3 md:grid-cols-2" onSubmit={event => { event.preventDefault(); save.mutate() }}>
          <label className="space-y-1 text-sm md:col-span-2">{t('https.hostsLabel')}<Input value={hosts} onChange={event => setHosts(event.currentTarget.value)} placeholder="recorder.example.com, 192.168.1.20" required /></label>
          <label className="flex items-center gap-2 text-sm md:col-span-2"><input type="checkbox" checked={redirectHTTP} onChange={event => setRedirectHTTP(event.currentTarget.checked)} />{t('https.redirect')}</label>
          <label className="flex items-center gap-2 text-sm md:col-span-2"><input type="checkbox" checked={useACME} onChange={event => setUseACME(event.currentTarget.checked)} />{t('https.acme')}</label>
          {!useACME && <><label className="space-y-1 text-sm">{t('https.certificate')}<textarea className="min-h-36 w-full rounded-md border border-border bg-background p-2 font-mono text-xs" value={certificate} onChange={event => setCertificate(event.currentTarget.value)} autoComplete="off" /></label>
          <label className="space-y-1 text-sm">{t('https.privateKey')}<textarea className="min-h-36 w-full rounded-md border border-border bg-background p-2 font-mono text-xs" value={privateKey} onChange={event => setPrivateKey(event.currentTarget.value)} autoComplete="off" /></label></>}
          <div className="flex flex-wrap gap-2 md:col-span-2">
            <Button type="submit" disabled={save.isPending || !hosts.trim() || (useACME && parseHosts(hosts).length !== 1)}><Save className="h-4 w-4" />{useACME ? t('https.enableACME') : certificate || privateKey ? t('https.saveCertificate') : t('https.createSelfSigned')}</Button>
            {value.enabled && <Button type="button" variant="outline" disabled={disable.isPending} onClick={() => disable.mutate()}>{t('https.disable')}</Button>}
          </div>
        </form>
        {(save.error || disable.error || enableSaved.error) && <p role="alert" className="text-sm text-destructive">{errorMessage(save.error ?? disable.error ?? enableSaved.error)}</p>}
        <p className="text-xs leading-5 text-muted-foreground">{t('https.warning')}</p>
      </>}
    </CardContent>
  </Card>
}

function parseHosts(value: string) { return value.split(/[\s,;]+/).map(host => host.trim()).filter(Boolean) }
