import { request } from './client'
import { moduleMeta } from '@/data/modules'
import type { ActionResult, EntryRow, OverviewResult, PageResult } from '@/data/types'

export { moduleMeta }

export async function listEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<PageResult> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}?${query}`)
  if (!response.ok) throw new Error('业务数据读取失败')
  return await response.json()
}

export async function createEntry(
  key: string,
  values: Record<string, string>,
): Promise<ActionResult> {
  const response = await request(`/api/${key}`, {
    method: 'POST',
    body: JSON.stringify({ values }),
  })
  return await response.json()
}

export async function runAction(
  key: string,
  id: number,
  action: string,
): Promise<ActionResult> {
  const response = await request(`/api/${key}/${id}/actions`, {
    method: 'POST',
    body: JSON.stringify({ action }),
  })
  return await response.json()
}

export async function downloadEntries(
  key: string,
  filters: Record<string, string> = {},
): Promise<void> {
  const query = new URLSearchParams(filters).toString()
  const response = await request(`/api/${key}/export?${query}`)
  if (!response.ok) throw new Error('业务数据导出失败')
  const url = URL.createObjectURL(await response.blob())
  const anchor = document.createElement('a')
  anchor.href = url
  anchor.download = `${key}.csv`
  document.body.appendChild(anchor)
  anchor.click()
  document.body.removeChild(anchor)
  URL.revokeObjectURL(url)
}

export async function loadOverview(): Promise<OverviewResult> {
  const response = await request('/api/overview')
  if (!response.ok) throw new Error('运营概览读取失败')
  return await response.json()
}

export type RouteGrant = {
  grant_id: string
  target_id: string
  database: string
  table: string
  qualified_table: string
  enabled: boolean
  channels: string[]
  revoked_at: string
  reason: string
}

export type ReplayMember = {
  event_id: string
  source_row: string
  filter_passed: boolean
  prepared_targets: string[]
  prepared_rejected: { target_id: string; reason: string; stage: string }[]
  sent_targets: string[]
  rejected: { target_id: string; reason: string; stage: string }[]
  already_delivered: { target_id: string }[]
  outcome: string
}

export type ReplayReport = {
  replay_id: string
  targets: string[]
  primary_target: string
  primary_timed_out: boolean
  unavailable_targets: string[]
  summary: {
    members: number
    delivered: number
    rejected_authorization: number
    target_unavailable: number
    filtered: number
    rejected: number
    skipped_already_delivered: number
    tables: string[]
    targets: string[]
  }
  by_table: Record<
    string,
    {
      source_row: string
      members: number
      delivered_targets: string[]
      rejected_targets: string[]
      rejected_reasons: string[]
      already_delivered: string[]
    }
  >
  members: ReplayMember[]
  receipts: {
    receipt_key: string
    event_id: string
    source_db: string
    source_table: string
    target_id: string
    grant_id: string
    grant_revoked: boolean
    delivered_at: string
  }[]
}

export async function listRouteGrants(target = ''): Promise<RouteGrant[]> {
  const response = await request(`/api/route_dispatcher/route-grants?target=${encodeURIComponent(target)}`)
  if (!response.ok) throw new Error('路由目标授权读取失败')
  const payload = await response.json()
  return payload.items as RouteGrant[]
}

export async function registerRouteGrant(values: {
  target_id: string
  database: string
  table: string
}): Promise<RouteGrant> {
  const response = await request('/api/route_dispatcher/route-grants', {
    method: 'POST',
    body: JSON.stringify(values),
  })
  if (!response.ok) throw new Error('路由目标授权登记失败')
  return await response.json()
}

export async function revokeRouteGrant(values: {
  target_id: string
  database: string
  table: string
  reason?: string
}): Promise<{ revoked_grant_ids: string[] }> {
  const response = await request('/api/route_dispatcher/route-grants/revoke', {
    method: 'POST',
    body: JSON.stringify(values),
  })
  if (!response.ok) throw new Error('路由目标授权撤回失败')
  return await response.json()
}

export async function replayBatch(payload: unknown): Promise<ReplayReport> {
  const response = await request('/api/route_dispatcher/replay', {
    method: 'POST',
    body: JSON.stringify(payload),
  })
  if (!response.ok) throw new Error('批量回放失败')
  return await response.json()
}
