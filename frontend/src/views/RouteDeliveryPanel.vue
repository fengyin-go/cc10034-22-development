<template>
  <section class="route-panel">
    <header class="panel-head">
      <div>
        <h3>路由目标授权与回放</h3>
        <p class="page-desc">
          按接收方核对库表归属，主目标不可用时备用目标独立投递；已送达的源行保留原回执，重发不重复交付。
        </p>
      </div>
      <button class="btn" type="button" data-testid="reload-grants" @click="loadGrants">刷新授权</button>
    </header>

    <form class="filter-bar" @submit.prevent="submitGrant">
      <label class="filter-item">
        <span>接收方目标</span>
        <input v-model="grantForm.target_id" data-testid="grant-target" placeholder="standby_sink" required />
      </label>
      <label class="filter-item">
        <span>源数据库</span>
        <input v-model="grantForm.database" data-testid="grant-db" placeholder="finance" required />
      </label>
      <label class="filter-item">
        <span>源表</span>
        <input v-model="grantForm.table" data-testid="grant-table" placeholder="orders" required />
      </label>
      <button class="btn primary" data-testid="grant-submit" type="submit">登记表归属授权</button>
    </form>

    <table class="data-table">
      <thead>
        <tr><th>授权ID</th><th>接收方目标</th><th>库表</th><th>状态</th><th>撤回时间</th><th>操作</th></tr>
      </thead>
      <tbody>
        <tr v-for="grant in grants" :key="grant.grant_id">
          <td>{{ grant.grant_id }}</td>
          <td>{{ grant.target_id }}</td>
          <td>{{ grant.qualified_table }}</td>
          <td>{{ grant.enabled ? '生效中' : '已撤回' }}</td>
          <td>{{ grant.revoked_at || '—' }}</td>
          <td>
            <button
              v-if="grant.enabled"
              class="link"
              type="button"
              :data-testid="`revoke-${grant.target_id}-${grant.qualified_table}`"
              @click="revoke(grant)"
            >撤回授权</button>
            <span v-else>—</span>
          </td>
        </tr>
        <tr v-if="!grants.length">
          <td colspan="6" class="empty-state">暂无授权记录，先登记接收方的表归属</td>
        </tr>
      </tbody>
    </table>

    <h3>批量回放</h3>
    <form class="replay-form" @submit.prevent="submitReplay">
      <label class="filter-item">
        <span>回放ID</span>
        <input v-model="replayForm.replay_id" data-testid="replay-id" placeholder="r1" />
      </label>
      <label class="filter-item">
        <span>目标（逗号分隔，主目标在前）</span>
        <input v-model="replayForm.targets" data-testid="replay-targets" placeholder="primary_sink,standby_sink" />
      </label>
      <label class="filter-item">
        <span>主目标</span>
        <input v-model="replayForm.primary_target" data-testid="replay-primary" placeholder="primary_sink" />
      </label>
      <label class="check-item">
        <input v-model="replayForm.primary_timed_out" data-testid="replay-primary-timeout" type="checkbox" />
        <span>主目标本轮超时，不接收</span>
      </label>
      <label class="filter-item wide">
        <span>来源行（每行：事件ID, 库, 表）</span>
        <textarea
          v-model="replayForm.rows"
          data-testid="replay-rows"
          rows="4"
          placeholder="fin-1,finance,orders&#10;sales-1,sales,orders"
        />
      </label>
      <label class="filter-item wide">
        <span>回放前撤权（每行：目标, 库, 表）</span>
        <textarea
          v-model="replayForm.revoke"
          data-testid="replay-revoke"
          rows="2"
          placeholder="standby_sink,finance,orders"
        />
      </label>
      <button class="btn primary" data-testid="replay-submit" type="submit" :disabled="replaying">
        {{ replaying ? '回放中…' : '发起回放' }}
      </button>
    </form>
    <p v-if="message" class="error-text" data-testid="route-message">{{ message }}</p>

    <section v-if="report" class="replay-report" data-testid="replay-report">
      <h3>回放结果 {{ report.replay_id }}</h3>
      <div class="stat-row">
        <article class="stat-card"><span class="stat-label">回放成员</span><strong class="stat-value">{{ report.summary.members }}</strong></article>
        <article class="stat-card"><span class="stat-label">实际送达</span><strong class="stat-value">{{ report.summary.delivered }}</strong></article>
        <article class="stat-card"><span class="stat-label">授权拒绝</span><strong class="stat-value">{{ report.summary.rejected_authorization }}</strong></article>
        <article class="stat-card"><span class="stat-label">主端不可用</span><strong class="stat-value">{{ report.summary.target_unavailable }}</strong></article>
        <article class="stat-card"><span class="stat-label">回执跳过</span><strong class="stat-value">{{ report.summary.skipped_already_delivered }}</strong></article>
      </div>
      <p class="page-desc">
        主目标：{{ report.primary_target || '—' }}
        <span v-if="report.primary_timed_out">（本轮超时，未接收）</span>
      </p>

      <table class="data-table">
        <thead>
          <tr><th>来源表</th><th>成员数</th><th>实际抵达目标</th><th>被拒目标</th><th>回执跳过</th></tr>
        </thead>
        <tbody>
          <tr v-for="(row, table) in report.by_table" :key="table">
            <td>{{ table }}</td>
            <td>{{ row.members }}</td>
            <td>{{ row.delivered_targets.join('、') || '—' }}</td>
            <td>
              <span v-for="reason in row.rejected_reasons" :key="reason" class="reject-reason">{{ reason }}</span>
              <template v-if="!row.rejected_reasons.length">—</template>
            </td>
            <td>{{ row.already_delivered.join('、') || '—' }}</td>
          </tr>
        </tbody>
      </table>

      <h3>投递回执</h3>
      <table class="data-table">
        <thead>
          <tr><th>源行</th><th>目标</th><th>授权</th><th>送达时间</th><th>授权状态</th></tr>
        </thead>
        <tbody>
          <tr v-for="receipt in report.receipts" :key="receipt.receipt_key">
            <td>{{ receipt.source_db }}.{{ receipt.source_table }} / {{ receipt.event_id }}</td>
            <td>{{ receipt.target_id }}</td>
            <td>{{ receipt.grant_id }}</td>
            <td>{{ receipt.delivered_at }}</td>
            <td>{{ receipt.grant_revoked ? '原授权已撤回，回执保留' : '授权有效' }}</td>
          </tr>
          <tr v-if="!report.receipts.length">
            <td colspan="5" class="empty-state">暂无回执</td>
          </tr>
        </tbody>
      </table>
    </section>
  </section>
</template>

<script setup lang="ts">
import { onMounted, reactive, ref } from 'vue'

import {
  listRouteGrants,
  registerRouteGrant,
  replayBatch,
  revokeRouteGrant,
  type ReplayReport,
  type RouteGrant,
} from '@/api/backend-service'

const grants = ref<RouteGrant[]>([])
const message = ref('')
const replaying = ref(false)
const report = ref<ReplayReport | null>(null)
const grantForm = reactive({ target_id: '', database: '', table: '' })
const replayForm = reactive({
  replay_id: '',
  targets: 'primary_sink,standby_sink',
  primary_target: 'primary_sink',
  primary_timed_out: true,
  rows: 'fin-1,finance,orders\nsales-1,sales,orders',
  revoke: 'standby_sink,finance,orders',
})

function parseLines(text: string): string[][] {
  return text
    .split('\n')
    .map((line) => line.split(',').map((part) => part.trim()))
    .filter((parts) => parts.length >= 3 && parts.every(Boolean))
}

async function loadGrants() {
  try {
    grants.value = await listRouteGrants()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '授权读取失败'
  }
}

async function submitGrant() {
  message.value = ''
  try {
    await registerRouteGrant({ ...grantForm })
    await loadGrants()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '授权登记失败'
  }
}

async function revoke(grant: RouteGrant) {
  message.value = ''
  try {
    await revokeRouteGrant({
      target_id: grant.target_id,
      database: grant.database,
      table: grant.table,
      reason: '页面撤回',
    })
    await loadGrants()
  } catch (error) {
    message.value = error instanceof Error ? error.message : '授权撤回失败'
  }
}

async function submitReplay() {
  replaying.value = true
  message.value = ''
  try {
    report.value = await replayBatch({
      replay_id: replayForm.replay_id || null,
      targets: replayForm.targets.split(',').map((item) => item.trim()).filter(Boolean),
      primary_target: replayForm.primary_target.trim(),
      primary_timed_out: replayForm.primary_timed_out,
      rows: parseLines(replayForm.rows).map(([event_id, source_db, source_table]) => ({
        event_id,
        source_db,
        source_table,
      })),
      revoke_before_send: parseLines(replayForm.revoke).map(([target_id, database, table]) => ({
        target_id,
        database,
        table,
      })),
    })
  } catch (error) {
    message.value = error instanceof Error ? error.message : '批量回放失败'
  } finally {
    replaying.value = false
  }
}

onMounted(loadGrants)
</script>

<style scoped>
.route-panel { margin-top: 24px; border-top: 2px solid var(--border); padding-top: 16px; }
.panel-head { display: flex; justify-content: space-between; align-items: flex-start; }
.replay-form { display: flex; flex-wrap: wrap; gap: 10px; align-items: flex-end; margin-bottom: 12px; }
.filter-item.wide { flex-basis: 100%; }
.filter-item textarea { width: 100%; border: 1px solid var(--border); border-radius: 6px; padding: 8px; font-family: monospace; font-size: 12px; }
.check-item { display: flex; gap: 6px; align-items: center; font-size: 13px; }
.replay-report { margin-top: 16px; }
.reject-reason { display: block; font-size: 12px; color: #b42318; }
</style>
