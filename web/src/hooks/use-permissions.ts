import { useQuery } from '@tanstack/react-query'
import { accountAuthorizationQuery, sessionQuery } from '@/api/queries'

export function usePermissions() {
  const session = useQuery(sessionQuery)
  const authorization = useQuery({ ...accountAuthorizationQuery, enabled: session.data?.auth_enabled !== false })
  const can = (permission: string) => session.data?.auth_enabled === false || authorization.data?.permissions?.includes(permission) === true
  return { can, authorization, session }
}
