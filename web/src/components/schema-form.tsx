import { useEffect, useId, useMemo, useState } from 'react'
import { Eye, EyeOff, KeyRound, RotateCcw } from 'lucide-react'
import type { Schema, SchemaField } from '@/types/api'
import { defaultValues, fieldLabel, isVisible, validateSchema } from '@/lib/schema'
import { Button } from '@/components/ui/button'
import { Input, Textarea } from '@/components/ui/input'
import { Label } from '@/components/ui/label'
import { Select, SelectItem } from '@/components/ui/select'
import { Badge } from '@/components/ui/badge'
import { useI18n } from '@/i18n/provider'

type Props = {
  schema: Schema
  initialValues?: Record<string, unknown>
  storedValues?: Record<string, unknown>
  initialSecrets?: Record<string, { configured: boolean }> | Record<string, boolean>
  valueSources?: Record<string, string>
  secretSources?: Record<string, string>
  mode?: 'input' | 'config' | 'challenge' | 'watch'
  submitLabel?: string
  onSubmit: (result: { values: Record<string, unknown>; secrets: Record<string, string>; clearValues: string[]; clearSecrets: string[]; persistFields: string[] }) => void | Promise<void>
  busy?: boolean
}
export function SchemaForm({ schema, initialValues, storedValues, initialSecrets, valueSources, secretSources, mode = 'input', submitLabel = '저장', onSubmit, busy }: Props) {
  const { t } = useI18n()
  const initial = useMemo(() => ({ ...defaultValues(schema), ...(initialValues ?? {}) }), [schema, initialValues])
  const [values, setValues] = useState<Record<string, unknown>>(initial)
  const [secrets, setSecrets] = useState<Record<string, string>>({})
  const [clearValues, setClearValues] = useState<string[]>([])
  const [clearSecrets, setClearSecrets] = useState<string[]>([])
  const [persistFields, setPersistFields] = useState<string[]>([])
  const [dirtyFields, setDirtyFields] = useState<string[]>([])
  const [errors, setErrors] = useState<Record<string, string>>({})
  const [showSecrets, setShowSecrets] = useState<Record<string, boolean>>({})
  const [formError, setFormError] = useState('')
  const formId = useId()
  useEffect(() => { setValues({ ...defaultValues(schema), ...(initialValues ?? {}) }); setSecrets({}); setErrors({}); setDirtyFields([]); setClearValues([]); setClearSecrets([]); setPersistFields([]); setShowSecrets({}); setFormError('') }, [schema, initialValues])

  const update = (key: string, value: unknown) => { setValues(current => ({ ...current, [key]: value })); setDirtyFields(current => current.includes(key) ? current : [...current, key]); setClearValues(current => current.filter(item => item !== key)); setErrors(current => ({ ...current, [key]: '' })) }
  const optionalPersistence = (field: SchemaField) => field.persistence?.mode === 'optional'
  const submit = async (event: React.FormEvent) => {
    event.preventDefault()
    const nextErrors = validateSchema(schema, values, secrets, mode !== 'config')
    if (mode === 'watch') {
      for (const field of schema.fields) {
        const secretInfo = initialSecrets?.[field.key]
        const configured = typeof secretInfo === 'boolean' ? secretInfo : secretInfo?.configured ?? false
        if (field.control === 'secret' && configured && !secrets[field.key] && !clearSecrets.includes(field.key)) delete nextErrors[field.key]
      }
    }
    setErrors(nextErrors)
    if (Object.keys(nextErrors).length) { setFormError('입력한 내용을 확인해 주세요.'); return }
    setFormError('')
    const visible = schema.fields.filter(field => field.control !== 'action' && field.control !== 'status' && isVisible(field.visible_when, values))
    const submittedValues: Record<string, unknown> = {}
    const submittedSecrets: Record<string, string> = {}
    for (const field of visible) {
      if (field.control === 'secret') { if (secrets[field.key]) submittedSecrets[field.key] = secrets[field.key] }
      else if (values[field.key] !== undefined && !clearValues.includes(field.key) && (mode !== 'config' || dirtyFields.includes(field.key))) submittedValues[field.key] = values[field.key]
    }
    const requiredPersistence = schema.fields.filter(field => field.persistence?.mode === 'required').map(field => field.key)
    await onSubmit({ values: submittedValues, secrets: submittedSecrets, clearValues, clearSecrets, persistFields: Array.from(new Set([...requiredPersistence, ...persistFields])) })
  }

  return <form className="space-y-4" onSubmit={submit} noValidate>
    {schema.fields.map(field => {
      if (!isVisible(field.visible_when, values)) return null
      const value = values[field.key]
      const source = valueSources?.[field.key]
      const localValue = storedValues ? Object.prototype.hasOwnProperty.call(storedValues, field.key) : Boolean(initialValues && Object.prototype.hasOwnProperty.call(initialValues, field.key))
      const secretInfo = initialSecrets?.[field.key]
      const configured = typeof secretInfo === 'boolean' ? secretInfo : secretInfo?.configured ?? false
      const inputId = `${formId}-${field.key}`
      const descriptionId = field.description ? `${inputId}-description` : undefined
      const errorId = errors[field.key] ? `${inputId}-error` : undefined
      const describedBy = [descriptionId, errorId].filter(Boolean).join(' ') || undefined
      return <div key={field.key} className="space-y-2 rounded-md">
        {field.control === 'action' ? <ActionField field={field} /> : field.control === 'status' ? <StatusField field={field} /> : <>
          <div className="flex items-center justify-between gap-3"><Label htmlFor={inputId}>{fieldLabel(field)}{field.required && <span className="ml-1 text-destructive" aria-label={t('schema.required')}>*</span>}</Label>
            {mode === 'config' && source && !localValue && <Badge tone="blue">상속 · {source}</Badge>}
          </div>
          {field.description && <p id={descriptionId} className="-mt-1 text-xs leading-5 text-muted-foreground">{field.description}</p>}
          {field.control === 'text' && <Input id={inputId} aria-describedby={describedBy} value={typeof value === 'string' ? value : ''} maxLength={field.constraints?.max_length} minLength={field.constraints?.min_length} onChange={event => update(field.key, event.target.value)} aria-invalid={Boolean(errors[field.key])} />}
          {field.control === 'textarea' && <Textarea id={inputId} aria-describedby={describedBy} value={typeof value === 'string' ? value : ''} maxLength={field.constraints?.max_length} minLength={field.constraints?.min_length} onChange={event => update(field.key, event.target.value)} aria-invalid={Boolean(errors[field.key])} />}
          {field.control === 'number' && <Input id={inputId} aria-describedby={describedBy} type="number" value={typeof value === 'number' ? value : ''} min={field.constraints?.min} max={field.constraints?.max} onChange={event => update(field.key, event.target.value === '' ? undefined : Number(event.target.value))} aria-invalid={Boolean(errors[field.key])} />}
          {field.control === 'boolean' && <label className="flex min-h-9 items-center gap-2 text-sm"><input id={inputId} aria-describedby={describedBy} type="checkbox" checked={typeof value === 'boolean' ? value : false} onChange={event => update(field.key, event.target.checked)} className="h-4 w-4 accent-primary" />{field.description || '사용'} </label>}
          {field.control === 'select' && <Select id={inputId} aria-describedby={describedBy} aria-invalid={Boolean(errors[field.key])} value={value === undefined ? undefined : encodeOption(value)} onValueChange={encoded => update(field.key, decodeOption(encoded, field.options ?? []))} placeholder="선택하세요"><SelectItem value="__none">선택하세요</SelectItem>{(field.options ?? []).map((option, index) => <SelectItem key={`${field.key}-${index}`} value={encodeOption(option.value)}>{option.label}</SelectItem>)}</Select>}
          {field.control === 'multi-select' && <div className="space-y-2 rounded-md border border-input bg-background p-2.5" role="group" aria-label={fieldLabel(field)}>{(field.options ?? []).map((option, index) => { const current = Array.isArray(value) ? value : []; const selected = current.some(item => stableJSON(item) === stableJSON(option.value)); return <label key={`${field.key}-${index}`} className="flex cursor-pointer items-center gap-2 rounded px-1.5 py-1.5 text-sm hover:bg-muted"><input type="checkbox" checked={selected} onChange={event => update(field.key, event.target.checked ? [...current, option.value] : current.filter(item => stableJSON(item) !== stableJSON(option.value)))} className="h-4 w-4 accent-primary" />{option.label}</label> })}</div>}
          {field.control === 'secret' && <div className="relative"><KeyRound className="pointer-events-none absolute left-3 top-1/2 h-4 w-4 -translate-y-1/2 text-muted-foreground" /><Input id={inputId} aria-describedby={describedBy} type={showSecrets[field.key] ? 'text' : 'password'} value={secrets[field.key] ?? ''} placeholder={configured ? '설정된 값이 있습니다 · 새 값 입력 시 교체' : ''} autoComplete="new-password" className="pl-9 pr-10" onChange={event => { setSecrets(current => ({ ...current, [field.key]: event.target.value })); setClearSecrets(current => current.filter(x => x !== field.key)); setErrors(current => ({ ...current, [field.key]: '' })) }} aria-invalid={Boolean(errors[field.key])} /><button type="button" className="focus-ring absolute right-2 top-1/2 -translate-y-1/2 rounded p-1 text-muted-foreground" aria-label={showSecrets[field.key] ? '비밀 값 숨기기' : '비밀 값 표시'} onClick={() => setShowSecrets(current => ({ ...current, [field.key]: !current[field.key] }))}>{showSecrets[field.key] ? <EyeOff className="h-4 w-4" /> : <Eye className="h-4 w-4" />}</button></div>}
          {field.control === 'secret' && configured && (mode === 'config' || mode === 'watch') && <div className="flex items-center justify-between text-xs"><span className="text-muted-foreground">● 설정됨{secretSources?.[field.key] ? ` · ${secretSources[field.key]}` : ''}</span><button type="button" className="font-medium text-destructive hover:underline" onClick={() => { setClearSecrets(current => current.includes(field.key) ? current.filter(x => x !== field.key) : [...current, field.key]); setSecrets(current => ({ ...current, [field.key]: '' })) }}>{clearSecrets.includes(field.key) ? '삭제 취소' : mode === 'watch' ? '비밀 값 삭제' : '명시적으로 삭제'}</button></div>}
          {mode === 'config' && field.control !== 'secret' && <div className="flex items-center justify-between gap-2 text-xs text-muted-foreground">{source && !localValue ? <span>현재 값은 상위 범위에서 적용 중입니다.</span> : localValue ? <span>이 범위에 직접 저장된 값</span> : <span>기본값 또는 미설정</span>}{localValue && <button type="button" className="inline-flex items-center gap-1 font-medium text-primary hover:underline" onClick={() => { setClearValues(current => current.includes(field.key) ? current.filter(x => x !== field.key) : [...current, field.key]); setDirtyFields(current => current.filter(x => x !== field.key)); if (!clearValues.includes(field.key)) setValues(current => ({ ...current, [field.key]: initialValues?.[field.key] ?? defaultValues(schema)[field.key] })) }}><RotateCcw className="h-3 w-3" />{clearValues.includes(field.key) ? '재정의 유지' : '상속/기본값 사용'}</button>}</div>}
          {mode === 'challenge' && field.persistence?.mode === 'required' && <p className="text-xs text-muted-foreground">이 값은 어댑터 정책에 따라 다음 확인에도 저장됩니다 · {persistenceTarget(field)}</p>}
          {mode === 'challenge' && optionalPersistence(field) && <label className="flex items-center gap-2 text-xs text-muted-foreground"><input type="checkbox" checked={persistFields.includes(field.key)} onChange={event => setPersistFields(current => event.target.checked ? [...current, field.key] : current.filter(key => key !== field.key))} className="accent-primary" />다음에도 저장 · {persistenceTarget(field)}</label>}
          {errors[field.key] && <p id={errorId} className="text-xs text-destructive" role="alert">{errors[field.key]}</p>}
        </>}
      </div>
    })}
    {formError && <p role="alert" className="rounded-md bg-destructive/10 px-3 py-2 text-sm text-destructive">{formError}</p>}
    <Button type="submit" disabled={busy}>{busy ? '처리 중…' : submitLabel}</Button>
  </form>
}

function ActionField({ field }: { field: SchemaField }) { return <div className="rounded-md border border-border bg-muted/30 p-3" role="note"><p className="text-sm font-semibold">{field.label}</p>{field.description && <p className="mt-1 text-xs text-muted-foreground">{field.description}</p>}<p className="mt-2 text-[11px] text-muted-foreground">프로토콜에서 실행 동작이 정의되지 않아 정보로만 표시합니다.</p></div> }
function stableJSON(value: unknown): string { if (Array.isArray(value)) return `[${value.map(stableJSON).join(',')}]`; if (value && typeof value === 'object') return `{${Object.entries(value).sort(([a], [b]) => a.localeCompare(b)).map(([key, item]) => `${JSON.stringify(key)}:${stableJSON(item)}`).join(',')}}`; return JSON.stringify(value) }
function encodeOption(value: unknown) { return stableJSON(value) }
function decodeOption(encoded: string, options: { value: unknown }[]) { return options.find(option => encodeOption(option.value) === encoded)?.value }
function persistenceTarget(field: SchemaField) { const scope = field.persistence?.target.scope; return scope === 'plugin' ? '어댑터 공통' : scope === 'resource' ? field.persistence?.target.resource ? `${field.persistence.target.resource.resource_type} 리소스` : '선언된 리소스' : '현재 리소스' }
function StatusField({ field }: { field: SchemaField }) { return <div className="rounded-md bg-muted/50 px-3 py-2"><p className="text-xs font-medium text-muted-foreground">{field.label} · 표시 정보</p>{field.description && <p className="mt-1 text-sm">{field.description}</p>}</div> }
