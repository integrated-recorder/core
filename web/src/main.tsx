import React from 'react'
import ReactDOM from 'react-dom/client'
import { QueryClient, QueryClientProvider } from '@tanstack/react-query'
import { RouterProvider } from '@tanstack/react-router'
import { router } from './routes'
import { setCSRFToken } from '@/api/client'
import { ToastProvider } from '@/components/ui/toast'
import { I18nProvider } from '@/i18n/provider'
import './index.css'

const queryClient = new QueryClient({ defaultOptions: { queries: { staleTime: 15_000, retry: (count, error) => count < 1 && !(error instanceof Error && 'status' in error && (error as { status: number }).status === 401), refetchOnWindowFocus: true }, mutations: { retry: false } } })
router.update({ context: { queryClient } })
window.addEventListener('ir:unauthorized', () => {
  setCSRFToken(undefined)
  queryClient.setQueryData(['auth', 'session'], { auth_enabled: true, authenticated: false, needs_bootstrap: false })
  void router.navigate({ to: '/login', search: {} })
})

ReactDOM.createRoot(document.getElementById('root')!).render(
  <React.StrictMode><QueryClientProvider client={queryClient}><I18nProvider><ToastProvider><RouterProvider router={router} /></ToastProvider></I18nProvider></QueryClientProvider></React.StrictMode>,
)
