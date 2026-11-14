<script setup>
import { onMounted, ref } from 'vue'
import { getJSON, postJSON } from '../api'

const streams = ref([])
const streamId = ref(null)
const fixtures = ref([])
const fixturePath = ref('')
const imports_ = ref([])
const recon = ref(null)
const last = ref(null)
const busy = ref(false)
const err = ref('')
const limit = ref('')

async function loadBase() {
  streams.value = await getJSON('/api/streams')
  fixtures.value = await getJSON('/api/snapshots/fixtures')
  imports_.value = await getJSON('/api/snapshots/imports')
  if (streamId.value == null && streams.value.length) {
    const lane = streams.value.find((s) => s.snapshot_import_id) || streams.value[0]
    streamId.value = lane.id
  }
  if (!fixturePath.value && fixtures.value.length) fixturePath.value = fixtures.value[0]
}

async function loadRecon() {
  if (streamId.value == null) return
  try {
    recon.value = await getJSON(`/api/snapshots/reconcile/${streamId.value}`)
  } catch (e) {
    recon.value = null
    err.value = String(e.message || e)
  }
}

async function refreshAll() {
  err.value = ''
  await loadBase()
  await loadRecon()
}

async function doImport() {
  busy.value = true
  err.value = ''
  try {
    await postJSON('/api/snapshots/import', {
      stream_id: streamId.value,
      fixture_path: fixturePath.value,
    })
    await refreshAll()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function doCatchup() {
  busy.value = true
  err.value = ''
  try {
    const body = { stream_id: streamId.value }
    const n = Number.parseInt(limit.value, 10)
    if (Number.isFinite(n) && n > 0) body.limit = n
    last.value = await postJSON('/api/snapshots/catchup', body)
    await refreshAll()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

async function doReset() {
  busy.value = true
  err.value = ''
  try {
    await postJSON('/api/snapshots/reset', { stream_id: streamId.value })
    last.value = null
    await refreshAll()
  } catch (e) {
    err.value = String(e.message || e)
  } finally {
    busy.value = false
  }
}

function statusClass(status) {
  if (status === 'caught_up') return 'ok'
  if (status === 'catching_up') return 'warn'
  return 'warn'
}

// Sink 对账表：实际/预期取并集，缺哪边就显示缺失，便于一眼看出同键脏行
function sinkKeys(recon) {
  const keys = new Set([
    ...Object.keys(recon.sink.actual || {}),
    ...Object.keys(recon.sink.expected || {}),
  ])
  return [...keys].sort()
}

function sinkRowMismatch(recon, key) {
  const a = recon.sink.actual[key]
  const e = recon.sink.expected[key]
  return JSON.stringify(a) !== JSON.stringify(e)
}

onMounted(refreshAll)
</script>
<template>
  <div class="page">
    <h1>快照导入与 Live 追平</h1>
    <p class="muted">
      先把某时刻的实体快照灌进 Sink 并记下水位（watermark_seq），再追 live CDC；
      重叠区事件按公布规则跳过/幂等吞掉，游标到 live 末端才标记「已追上」。
    </p>

    <div class="row-actions panel">
      <label>流
        <select v-model.number="streamId" @change="loadRecon">
          <option v-for="s in streams" :key="s.id" :value="s.id">
            {{ s.id }} · {{ s.name }}（游标 {{ s.apply_cursor }}<template v-if="s.snapshot_import_id"> · 快照 #{{ s.snapshot_import_id }}</template>）
          </option>
        </select>
      </label>
      <label>快照夹具
        <select v-model="fixturePath">
          <option v-for="f in fixtures" :key="f" :value="f">{{ f }}</option>
        </select>
      </label>
      <button :disabled="busy" @click="doImport">① 导入快照</button>
      <label class="muted">本批上限（可空=全量）
        <input v-model="limit" style="width: 6rem" placeholder="如 3" />
      </label>
      <button :disabled="busy" @click="doCatchup">② 追 Live</button>
      <button class="ghost" :disabled="busy" @click="doReset">重置该 Lane</button>
    </div>
    <p v-if="err" class="tag danger">{{ err }}</p>

    <div class="panel" v-if="recon">
      <h3>
        追平状态：
        <span class="tag" :class="statusClass(recon.status)">
          {{ recon.status === 'caught_up' ? '已追上 caught_up' : recon.status === 'catching_up' ? '追平中 catching_up' : recon.status }}
        </span>
        <span class="muted" style="margin-left: .6rem">
          游标 {{ recon.cursor.apply_cursor }} / 快照界 watermark {{ recon.cursor.watermark_seq ?? '—' }} /
          live 最大 seq {{ recon.cursor.live_max_seq }}
        </span>
      </h3>
      <p class="muted" v-if="recon.status !== 'caught_up'">
        尚未追上：游标 {{ recon.cursor.apply_cursor }} 仍在 live 末端 {{ recon.cursor.live_max_seq }} 之前，
        不标完成、不写追上时间；继续「追 Live」直到全量 live seq 消化完毕。
      </p>
      <div class="tiles">
        <div class="tile"><div class="hero-num">{{ recon.snapshot_record.row_count }}</div><div class="muted">快照行数</div></div>
        <div class="tile"><div class="hero-num">{{ recon.snapshot_record.live_candidate_count }}</div><div class="muted">live 候选（已消化 seq）</div></div>
        <div class="tile"><div class="hero-num">{{ recon.snapshot_record.overlap_skipped_count }}</div><div class="muted">重叠跳过（不落 Sink）</div></div>
        <div class="tile"><div class="hero-num">{{ recon.snapshot_record.applied_count }}</div><div class="muted">真正 applied</div></div>
        <div class="tile"><div class="hero-num">{{ recon.snapshot_record.duplicate_skipped_count }}</div><div class="muted">重复幂等吞掉</div></div>
        <div class="tile"><div class="hero-num">{{ recon.snapshot_record.cursor_after }}</div><div class="muted">快照记录游标</div></div>
      </div>
      <h4>四方对账（快照记录 / 账本 / Sink / 游标 / 状态机）</h4>
      <p>
        <span class="tag" :class="recon.all_match ? 'ok' : 'danger'">
          {{ recon.all_match ? '全部对上号' : '存在未对上项' }}
        </span>
        <span class="muted" style="margin-left: .6rem">
          追平中与追上后都应全绿：重叠跳过不得引入额外 Sink 脏行，未追上不得伪造游标。
        </span>
      </p>
      <table>
        <thead><tr><th>核对项</th><th>结果</th><th>明细</th></tr></thead>
        <tbody>
          <tr v-for="c in recon.checks" :key="c.name">
            <td>{{ c.name }}</td>
            <td><span class="tag" :class="c.ok ? 'ok' : 'danger'">{{ c.ok ? 'OK' : '不成立' }}</span></td>
            <td class="muted mono">{{ c.detail }}</td>
          </tr>
        </tbody>
      </table>
      <p class="muted" style="margin-top: .6rem">规则：{{ recon.rules }}</p>
    </div>

    <div class="panel" v-if="last">
      <h3>最近一次追平 Run</h3>
      <p>
        run #{{ last.run.id }} ·
        <span class="tag" :class="last.run.status === 'caught_up' ? 'ok' : 'warn'">{{ last.run.status }}</span>
        · loaded {{ last.run.loaded_count }} / applied {{ last.run.applied_count }} /
        skipped {{ last.run.skipped_count }} · cursor {{ last.run.cursor_before }} → {{ last.run.cursor_after }}
      </p>
    </div>

    <div class="panel">
      <h3>快照导入记录（可回看，落库持久化）</h3>
      <table>
        <thead>
          <tr>
            <th>ID</th><th>流</th><th>snapshot_id</th><th>快照时间</th><th>watermark</th>
            <th>状态</th><th>快照行</th><th>live候选</th><th>重叠跳过</th><th>applied</th><th>游标</th><th>追上时间</th>
          </tr>
        </thead>
        <tbody>
          <tr v-for="r in imports_" :key="r.id">
            <td>{{ r.id }}</td>
            <td>{{ r.stream_id }}</td>
            <td><code>{{ r.snapshot_id }}</code></td>
            <td>{{ r.snapshot_ts }}</td>
            <td>{{ r.watermark_seq }}</td>
            <td><span class="tag" :class="statusClass(r.status)">{{ r.status }}</span></td>
            <td>{{ r.row_count }}</td>
            <td>{{ r.live_candidate_count }}</td>
            <td>{{ r.overlap_skipped_count }}</td>
            <td>{{ r.applied_count }}</td>
            <td>{{ r.cursor_before }}→{{ r.cursor_after }}</td>
            <td>{{ r.caught_up_at || '—' }}</td>
          </tr>
          <tr v-if="!imports_.length"><td colspan="12" class="muted">暂无快照导入</td></tr>
        </tbody>
      </table>
    </div>

    <div class="panel" v-if="recon">
      <h3>Sink 对账：实际态 vs 预期态（快照 + 已 applied）</h3>
      <p class="muted">
        实际 {{ recon.sink.actual_count }} 行 / 预期 {{ recon.sink.expected_count }} 行；
        重叠区跳过是纯读侧裁决——不重放旧 upsert（不让已删实体复活），也不重放旧 delete
        （不抹掉快照行），同键不会多出脏行。{{
          recon.sink.dirty_overlap_keys && recon.sink.dirty_overlap_keys.length
            ? `脏键：${recon.sink.dirty_overlap_keys.join('、')}`
            : '脏键：无'
        }}
      </p>
      <table>
        <thead><tr><th>entity_key</th><th>实际 payload</th><th>预期 payload</th><th>一致</th></tr></thead>
        <tbody>
          <tr v-for="key in sinkKeys(recon)" :key="key">
            <td>{{ key }}</td>
            <td><pre class="mono">{{ recon.sink.actual[key] === undefined ? '（Sink 中缺失）' : JSON.stringify(recon.sink.actual[key]) }}</pre></td>
            <td><pre class="mono">{{ recon.sink.expected[key] === undefined ? '（预期中不存在）' : JSON.stringify(recon.sink.expected[key]) }}</pre></td>
            <td>
              <span class="tag" :class="sinkRowMismatch(recon, key) ? 'danger' : 'ok'">
                {{ sinkRowMismatch(recon, key) ? '不一致' : 'OK' }}
              </span>
            </td>
          </tr>
          <tr v-if="!sinkKeys(recon).length"><td colspan="4" class="muted">Sink 为空</td></tr>
        </tbody>
      </table>
    </div>
  </div>
</template>
