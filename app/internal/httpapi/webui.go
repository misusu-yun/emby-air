package httpapi

import "net/http"

// handleWebUI 返回极简管理面板（纯 HTML + 原生 JS，无外部资源）。
func (s *Server) handleWebUI(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/html; charset=utf-8")
	// 管理页不缓存：升级后浏览器不会继续跑旧 JS。
	w.Header().Set("Cache-Control", "no-store")
	_, _ = w.Write([]byte(webUIHTML))
}

const webUIHTML = `<!doctype html><html lang="zh-CN"><head><meta charset="utf-8">
<meta name="viewport" content="width=device-width,initial-scale=1"><title>emby-air 控制台</title>
<style>
 body{background:#0f1216;color:#d7e0ea;font:13px/1.6 ui-monospace,Menlo,Consolas,monospace;margin:0;padding:20px;max-width:1200px}
 h1{font-size:16px;margin:0 0 10px} h2{font-size:13px;margin:18px 0 8px;color:#7fd1ff;border-bottom:1px solid #243040;padding-bottom:4px}
 table{border-collapse:collapse;width:100%} td,th{border-bottom:1px solid #1d2733;padding:5px 8px;text-align:left}
 th{color:#7d8b9a;font-weight:normal}
 input,select,textarea{background:#0b0f14;color:#d7e0ea;border:1px solid #2a3646;padding:3px 6px;font:12px ui-monospace,monospace}
 button{background:#1b2735;color:#9fd4ff;border:1px solid #2f465f;padding:3px 8px;cursor:pointer;font:12px ui-monospace,monospace;margin-right:4px}
 button:hover{background:#24374b}
 .muted{color:#66757f}.ok{color:#57d38c}.bad{color:#ff7b72}
 progress{width:220px;height:10px}
 .tabs button.active,#mlibs button.active{color:#0f1216;background:#7fd1ff}
 .avatar{width:32px;height:32px;border-radius:50%;object-fit:cover;background:#1b2735;vertical-align:middle}
 .avatar-empty{display:inline-flex;align-items:center;justify-content:center;color:#66757f;font-size:14px}
 #avCanvas{background:#0b0f14;border:1px solid #2a3646;cursor:grab;touch-action:none}
 #avCanvas:active{cursor:grabbing}
 pre{background:#121821;border:1px solid #243040;padding:8px;overflow:auto;margin:0;max-height:420px}
 .modal{position:fixed;inset:0;background:#000a;display:none;align-items:center;justify-content:center}
 .modal>div{background:#131a23;border:1px solid #2a3646;padding:16px;width:min(760px,92vw);max-height:86vh;overflow:auto}
 #toast{position:fixed;right:16px;bottom:16px;max-width:420px;display:none;padding:10px 14px;border:1px solid #2a3646;background:#131a23;white-space:pre-wrap;z-index:99}
 #toast.ok{border-color:#2c6a45;color:#8ef0b4} #toast.bad{border-color:#7a2c2c;color:#ff9a92}
</style></head><body>
<h1>emby-air 控制台</h1>
<div id="login">用户 <input id="u" value="admin" size="8"> 密码 <input id="p" type="password" size="12">
 <button onclick="login()">登录</button> <span id="msg" class="muted"></span></div>

<div id="main" style="display:none">
<div class="tabs"><button onclick="tab('libs')" id="t-libs" class="active">媒体库</button>
<button onclick="tab('status')" id="t-status">状态</button>
<button onclick="tab('media')" id="t-media">影片</button>
<button onclick="tab('users')" id="t-users">用户</button>
<button onclick="tab('jobs')" id="t-jobs">任务</button>
<button onclick="tab('logs')" id="t-logs">日志</button>
<button onclick="tab('keys')" id="t-keys">API Key</button>
<button onclick="tab('settings')" id="t-settings">设置</button></div>

<section id="s-libs">
<h2>媒体库</h2>
<table id="libs"><thead><tr><th style="width:52px">序号</th><th>封面</th><th>名称</th><th>类型</th><th>路径</th><th>操作</th></tr></thead><tbody></tbody></table>
<div style="margin-top:8px">扫描计划：每天 <input type="time" id="scanTime"> <button onclick="saveSchedule()">保存</button> <button onclick="clearSchedule()">关闭定时扫描</button> <span class=muted id="scanHint"></span></div>
<div style="margin-top:8px">新建：名称 <input id="lname" size="10"> 类型
<select id="ltype"><option value="movies">电影</option><option value="tvshows">剧集</option></select></div>
<div id="lpathRows" style="margin-top:6px"></div>
<div style="margin-top:8px"><button onclick="addLib()">创建</button> <span class="muted">支持勾选多个目录</span></div>
</section>

<section id="s-status" style="display:none">
<h2>正在播放 <span class="muted" id="nowcount"></span></h2>
<table id="nowplay"><thead><tr><th>用户</th><th>媒体</th><th>客户端</th><th>设备</th><th>开始</th><th>进度</th></tr></thead><tbody></tbody></table>
<div id="nowempty" class="muted">当前没有正在播放的会话</div>
<h2>活动 <span class="muted">最近 100 条</span></h2>
<div id="acts" class="muted">—</div>
</section>

<section id="s-media" style="display:none">
<h2>影片管理</h2>
<div id="mlibs" style="margin-bottom:8px"></div>
<div style="margin-bottom:8px">搜索 <input id="mq" size="30" placeholder="中文 / 全拼 / 首字母，如 流浪地球 / liulangdiqiu / lldq"
 oninput="debouncedMedia()"> <button onclick="loadMedia()">搜索</button>
 <span class="muted" id="mcount"></span></div>
<table id="media"><thead><tr><th>封面</th><th>标题</th><th>状态</th><th>源大小</th><th>简介</th><th>路径</th><th>操作</th></tr></thead><tbody></tbody></table>
<div id="mpager" style="margin-top:10px"></div>
</section>

<section id="s-users" style="display:none">
<h2>用户</h2>
<table id="users"><thead><tr><th>头像</th><th>名称</th><th>管理员</th><th>禁用</th><th>可访问库</th><th>操作</th></tr></thead><tbody></tbody></table>
<div style="margin-top:8px">新建：名称 <input id="uname" size="10"> 密码 <input id="upass" type="password" size="12">
<label><input id="uadmin" type="checkbox"> 管理员</label> <button onclick="addUser()">创建</button></div>
<div style="margin-top:8px"><label><input id="pubUsers" type="checkbox" checked onchange="savePublicUsers()"> 登录页显示用户列表</label>
<span class="muted">关闭后部分第三方客户端只能手动输入用户名登录</span></div>
</section>

<section id="s-jobs" style="display:none">
<h2>任务</h2>
<div id="jobRunning"></div>
<h2 style="font-size:12px">排队中</h2><div id="jobQueued" class="muted">无</div>
<h2 style="font-size:12px">已完成</h2><div id="jobDone" class="muted">无</div>
</section>

<section id="s-logs" style="display:none">
<h2>日志</h2>
<div style="margin-bottom:6px">显示最近
<button onclick="loadLogs(100)">100 条</button><button onclick="loadLogs(500)">500 条</button>
<label><input type="checkbox" id="autolog" checked onchange="toggleAutoLog()"> 自动刷新</label></div>
<pre id="logs">—</pre>
</section>

<section id="s-keys" style="display:none">
<h2>API Key</h2>
<div id="keystate" class="muted">加载中…</div>
<div style="margin-top:8px"><button onclick="newKey()">生成新 Key</button></div>
</section>

<section id="s-settings" style="display:none">
<h2>TMDB / 刮削设置</h2>
<div class="muted">保存后新任务立即使用；正在运行的任务沿用旧配置。API Key 不会回显。</div>
<table style="max-width:760px;margin-top:8px">
<tbody>
<tr><td style="width:160px">TMDB API Key</td><td><input id="setTmdbKey" type="password" size="42" autocomplete="new-password" placeholder="留空表示不修改">
 <label class="muted"><input id="setClearKey" type="checkbox"> 清除已保存 Key</label>
 <div class="muted" id="setTmdbKeyState"></div></td></tr>
<tr><td>TMDB 语言</td><td><input id="setTmdbLang" size="12" placeholder="zh-CN"></td></tr>
<tr><td>HTTP/HTTPS 代理</td><td><input id="setProxy" size="42" placeholder="http://host:port；留空表示不使用代理"></td></tr>
<tr><td>NO_PROXY</td><td><input id="setNoProxy" size="42" placeholder="localhost,127.0.0.1"></td></tr>
<tr><td>刮削速率</td><td><input id="setRate" type="number" min="1" max="15" step="1" style="width:80px"> req/s <span class="muted">建议 8–15</span></td></tr>
<tr><td>扫描后自动刮削</td><td><label><input id="setAutoScrape" type="checkbox"> 启用</label></td></tr>
<tr><td>写 NFO</td><td><label><input id="setNFO" type="checkbox"> 启用</label> <span class="muted">需要媒体目录可写</span></td></tr>
<tr><td>拼音索引定时重建</td><td><input id="setIndexTime" size="8" placeholder="03:10"> <span class="muted">留空关闭；格式 HH:MM</span>
 <button onclick="saveNameIndexTime()">保存时间</button>
 <button onclick="rebuildNameIndex()">立即全量重建</button>
 <span id="setIndexHint" class="muted"></span></td></tr>
</tbody></table>
<div style="margin-top:10px"><button onclick="saveSettings()">保存设置</button> <span id="setHint" class="muted"></span></div>
</section>
</div>

<div class="modal" id="modal"><div id="modalBody"></div></div>
<div id="toast"></div>

<script>
let T=localStorage.getItem('gem_token')||'', autoLog=true, logLimit=100, coverVer=Date.now();
function toast(msg, ok){const el=$('toast');el.textContent=msg;el.className=ok?'ok':'bad';el.style.display='block';
 clearTimeout(window.__tt);window.__tt=setTimeout(()=>{el.style.display='none'},4000);}
const $=id=>document.getElementById(id);
let es=null, esRetry=0, pollInFlight=false, initialized=false;
const AUTH_ERR=/HTTP 40(1|3)/;
function isAuthError(e){return !!e&&AUTH_ERR.test(String(e.message||e));}
function timeoutSignal(){try{return AbortSignal.timeout(8000)}catch(_){return undefined}}
function showLogin(msg){
 T='';try{localStorage.removeItem('gem_token')}catch(_){}
 if(es){es.close();es=null;}
 if(window.__esTimer){clearTimeout(window.__esTimer);window.__esTimer=null;}
 esRetry=0;
 $('main').style.display='none';$('login').style.display='block';
 $('msg').innerHTML=msg?'<span class=bad>'+esc(msg)+'</span>':'';
}
function fmtTime(ts){if(!ts)return '-';const d=new Date(ts*1000);return d.toLocaleTimeString('zh-CN',{hour12:false});}
function ago(ms){if(!ms)return '-';const s=Math.max(0,Math.round((Date.now()-ms)/1000));return s<60?s+' 秒前':Math.round(s/60)+' 分钟前';}
async function loadNow(){try{
 const raw=await api('/webui/api/sessions');
 const d=Array.isArray(raw)?raw:[];
 $('nowcount').textContent=d.length?('（'+d.length+'）'):'';
 $('nowempty').style.display=d.length?'none':'';
 $('nowplay').querySelector('tbody').innerHTML=d.map(x=>{
   const pct=Math.max(0,Math.min(100,Math.round(x.Percent||0)));
   return '<tr><td>'+esc(x.UserName)+'</td><td>'+esc(x.Title||x.ItemName)+'</td><td>'+esc(x.Client)+'</td>'+
   '<td class=muted>'+esc(x.Device||'-')+'</td><td class=muted>'+fmtTime(x.Started)+'</td>'+
   '<td><progress value="'+pct+'" max="100"></progress> '+pct+'%'+(x.IsPaused?' <span class=muted>暂停</span>':'')+'</td></tr>'}).join('');
 }catch(e){/* 只影响“正在播放”区域，不打断其它区域 */}}
async function loadActs(){try{
 const raw=await api('/webui/api/activities?Limit=100');
 const d=Array.isArray(raw)?raw:[];
 const names={login:'登录',play_start:'开始播放',play_stop:'停止播放',delete:'删除片源',delete_failed:'删除失败'};
 $('acts').innerHTML=d.length?d.map(a=>{
   const n=names[a.Kind]||a.Kind;
   const extra=a.ItemName?(' · '+esc(a.ItemName)):'';
   return '<div style="margin:3px 0"><span class=muted>'+ago(a.Date)+'</span> '+
   '<b>'+esc(a.UserName||'-')+'</b> '+esc(n)+extra+
   ' <span class=muted>'+esc(a.Client||'')+(a.Device?' · '+esc(a.Device):'')+'</span></div>'}).join(''):'<span class=muted>暂无活动</span>';
 }catch(e){/* 只影响“活动”区域 */}}
let mediaItems=[], mediaLib='', mediaPage=1, mediaLibs=[];
function debouncedMedia(){clearTimeout(window.__mt);window.__mt=setTimeout(()=>{mediaPage=1;loadMedia();},300);}
async function loadMediaLibraries(){try{
 const raw=await api('/webui/api/media/libraries');
 if(!Array.isArray(raw)){mediaLibs=[];toast('读取媒体库失败：'+((raw&&raw.error)||'未知错误'),false);return;}
 mediaLibs=raw;
 const btn=(id,label,extra)=>'<button class="'+(mediaLib===id?'active':'')+'" onclick="pickLib(\''+id+'\')">'+esc(label)+' <span class=muted>'+extra+'</span></button>';
 $('mlibs').innerHTML=btn('','全部', mediaLibs.reduce((a,b)=>a+(b.MovieCount||0)+(b.SeriesCount||0),0)+' 部')+
   mediaLibs.map(l=>btn(l.Id,l.Name, l.Type==='tvshows'
     ? ('剧集 '+l.SeriesCount+' · 单集 '+l.EpisodeCount)
     : ('电影 '+l.MovieCount))).join('');
 }catch(e){mediaLibs=[];toast('读取媒体库失败：'+(e&&e.message||e),false);}}
function pickLib(id){mediaLib=id;mediaPage=1;loadMediaLibraries();loadMedia();}
async function loadMedia(){try{
 const q=encodeURIComponent(($('mq')&&$('mq').value||'').trim());
 let d=await api('/webui/api/media?q='+q+'&library='+encodeURIComponent(mediaLib)+'&page='+mediaPage+'&limit=10');
 if(!d||!Array.isArray(d.Items)){toast('读取影片列表失败：'+((d&&d.error)||'未知错误'),false);d={Items:[],Total:0};}
 mediaItems=d.Items;
 $('mcount').textContent='共 '+(d.Total||0)+' 条'+(q?'（已按搜索词过滤）':'');
 $('media').querySelector('tbody').innerHTML=mediaItems.map(m=>{
   const img=m.ImageUrl?'<img src="'+m.ImageUrl+'&api_key='+T+'" loading="lazy" decoding="async" style="width:64px;height:90px;object-fit:cover;background:#1b2735">':'<span class=muted>无</span>';
   const cls=m.Status==='scraped'?'ok':(m.Status==='failed'?'bad':'muted');
   return '<tr><td>'+img+'</td><td>'+esc(m.Name)+(m.Year?' <span class=muted>('+m.Year+')</span>':'')+'</td>'+
   '<td class="'+cls+'">'+esc(m.StatusLabel)+'</td>'+
   '<td class=muted>'+fmtSize(m.Size)+' <button onclick="probeSize(\''+m.Id+'\')">获取</button></td>'+
   '<td class=muted style="max-width:320px">'+esc(m.Overview||'')+'</td>'+
   '<td class=muted style="max-width:280px;word-break:break-all;font-size:11px">'+esc(m.Path||'')+'</td>'+
   '<td><button onclick="mediaAct(\''+m.Id+'\',\'rescan\')">重新扫描</button>'+
   '<button onclick="mediaAct(\''+m.Id+'\',\'rescore\')">重新刮削</button>'+
   '<button onclick="mediaDelete(\''+m.Id+'\')" style="color:#ff9a92">删除</button></td></tr>';
 }).join('')||'<tr><td colspan=7 class=muted>没有匹配的影片</td></tr>';
 renderPager(d.Total||0);
 }catch(e){toast('读取影片列表失败：'+(e&&e.message||e),false);}}
function renderPager(total){
 const pages=Math.max(1,Math.ceil(total/10));
 $('mpager').innerHTML='<span class=muted>共 '+total+' 条 · 第 '+mediaPage+'/'+pages+' 页</span> '+
   '<button '+(mediaPage<=1?'disabled':'')+' onclick="gotoPage('+(mediaPage-1)+')">上一页</button>'+
   '<button '+(mediaPage>=pages?'disabled':'')+' onclick="gotoPage('+(mediaPage+1)+')">下一页</button>';
}
function gotoPage(p){mediaPage=Math.max(1,p);loadMedia();}
function fmtSize(n){if(!n)return '-';return (n/1024/1024/1024>=1?(n/1024/1024/1024).toFixed(2)+' GB':(n/1024/1024).toFixed(0)+' MB');}
async function probeSize(id){
 toast('探测源大小…',true);
 const r=await fetch('/webui/api/media/'+id+'/size',{method:'POST',headers:{'X-Emby-Token':T}});
 if(r.ok){toast('已获取源大小',true);loadMedia();}else{toast('获取失败：HTTP '+r.status,false);}
}
async function loadSchedule(){try{
 const d=await api('/webui/api/scan-schedule')||{};
 if($('scanTime')) $('scanTime').value=d.Time||'';
 if($('scanHint')) $('scanHint').textContent=d.Time?('每天 '+d.Time+' 扫描'):'当前未启用定时扫描';
 }catch(e){if($('scanHint'))$('scanHint').textContent='读取扫描计划失败';}}
async function saveSchedule(){
 const t=$('scanTime').value;
 const r=await fetch('/webui/api/scan-schedule',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify({Time:t})});
 if(r.ok){toast('扫描计划已保存',true);loadSchedule();}else{toast('保存失败：HTTP '+r.status,false);}
}
async function clearSchedule(){
 const r=await fetch('/webui/api/scan-schedule',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify({Time:''})});
 if(r.ok){toast('已关闭定时扫描',true);loadSchedule();}
}
async function mediaAct(id,action){
 const label=action==='rescan'?'重新扫描':'重新刮削';
 toast(label+'中…',true);
 const r=await fetch('/webui/api/media/'+id+'/'+action,{method:'POST',headers:{'X-Emby-Token':T}});
 if(r.status===202){toast(label+'已加入任务队列',true);loadJobs();}
 else if(r.ok){toast(label+'完成',true);loadMedia();}
 else{const t=await r.text();toast(label+'失败：'+(t||r.status),false);}
}
async function mediaDelete(id){
 const m=(mediaItems||[]).find(x=>x.Id===id)||{Name:'该影片'};
 if(!confirm('删除《'+m.Name+'》？\n将把整个影片目录移入回收站，并从媒体库移除。'))return;
 if(!confirm('再次确认：真的要删除《'+m.Name+'》吗？'))return;
 const r=await fetch('/webui/api/media/'+id,{method:'DELETE',headers:{'X-Emby-Token':T}});
 if(r.ok){const d=await r.json().catch(()=>({}));toast('已删除（条目 '+(d.items||1)+' 个，图片 '+(d.images||0)+' 张）',true);loadMedia();}
 else{const t=await r.text();toast('删除失败：'+(t||r.status),false);}
}

function connectEvents(){
 if(!T)return;
 if(es){es.close();es=null;}
 if(window.__esTimer){clearTimeout(window.__esTimer);window.__esTimer=null;}
 try{es=new EventSource('/webui/events?api_key='+encodeURIComponent(T));}catch(_){es=null;scheduleEsReconnect();return;}
 es.addEventListener('session',()=>{if($('s-status').style.display!=='none')loadNow();});
 es.addEventListener('activity',e=>{try{JSON.parse(e.data)}catch(_){}if($('s-status').style.display!=='none')loadActs();});
 es.onopen=()=>{esRetry=0;};
 es.onerror=()=>{if(es){es.close();es=null;}scheduleEsReconnect();};
}
// 只保留一个重连定时器，指数退避 5s→30s；令牌失效（401/403）时停止重连。
function scheduleEsReconnect(){
 if(!T||window.__esTimer)return;
 const delay=Math.min(30000,5000*Math.pow(2,esRetry));
 esRetry++;
 window.__esTimer=setTimeout(async()=>{
  window.__esTimer=null;
  if(!T)return;
  // EventSource 不暴露 HTTP 状态码：先探一次令牌，401/403 就不再重连。
  try{await api('/emby/Users/Me');}catch(e){if(isAuthError(e))return;}
  if(T)connectEvents();
 },delay);
}

const esc=s=>String(s==null?'':s).replace(/[&<>"]/g,c=>({'&':'&amp;','<':'&lt;','>':'&gt;','"':'&quot;'}[c]));
async function api(p,o={}){
 let r;
 try{r=await fetch(p,{...o,headers:{'Content-Type':'application/json','X-Emby-Token':T,...(o.headers||{})},signal:o.signal||timeoutSignal()});}
 catch(e){throw new Error('网络错误：'+((e&&e.message)||e));}
 // 401：令牌失效，统一清 token + 回登录框（不再吭哧解析错误体）。
 if(r.status===401){showLogin('登录已失效，请重新登录');throw new Error('HTTP 401');}
 if(r.status===403)throw new Error('HTTP 403');
 if(!r.ok){let t='';try{t=await r.text()}catch(_){};throw new Error('HTTP '+r.status+(t?'：'+t.slice(0,200):''));}
 if(r.status===204)return null;
 const t=await r.text();
 if(!t)return null;
 try{return JSON.parse(t)}catch{return t}
}
async function login(){let r,d={};
 try{r=await fetch('/webui/login',{method:'POST',headers:{'Content-Type':'application/json'},
  body:JSON.stringify({Name:$('u').value,Pw:$('p').value})});d=await r.json().catch(()=>({}));}
 catch(e){$('msg').innerHTML='<span class=bad>无法连接服务器</span>';return}
 if(!d.AccessToken){$('msg').innerHTML='<span class=bad>'+esc(d.error||('登录失败 HTTP '+(r&&r.status)))+'</span>';return}
 T=d.AccessToken;try{localStorage.setItem('gem_token',T)}catch(_){}
 $('msg').innerHTML='';esRetry=0;
 if(window.__esTimer){clearTimeout(window.__esTimer);window.__esTimer=null;}
 if(es){es.close();es=null;}
 initialized=false;startUI();}
function tab(n){for(const x of ['libs','status','media','users','jobs','logs','keys','settings']){$('s-'+x).style.display=x===n?'':'none';$('t-'+x).className=x===n?'active':''}
 if(n==='jobs')loadJobs(); if(n==='logs')loadLogs(logLimit); if(n==='keys')loadKeys(); if(n==='settings')loadSettings(); if(n==='status'){loadNow();loadActs();} if(n==='media'){mediaPage=1;loadMediaLibraries();loadMedia();} if(n==='libs')loadSchedule();}
async function refreshAll(){
 // 用 allSettled / 各自 try-catch：一个区域失败不能阻断其它区域。
 const tasks=[loadLibs,loadUsers,loadJobs];
 if($('s-keys').style.display!=='none')tasks.push(loadKeys);
 if($('s-logs').style.display!=='none')tasks.push(()=>loadLogs(logLimit));
 await Promise.allSettled(tasks.map(f=>f()));
}
// startUI 只跑一次：登录成功与本地 token 恢复共用，避免重复 refreshAll/多条 SSE 连接。
function startUI(){
 if(initialized)return;
 initialized=true;
 $('login').style.display='none';$('main').style.display='block';
 refreshAll();connectEvents();
}
let libsCache=[];
async function loadLibs(){try{
 const raw=await api('/emby/Library/VirtualFolders');
 libsCache=Array.isArray(raw)?raw:[];
 $('libs').querySelector('tbody').innerHTML=libsCache.map((v,idx)=>{
  const paths=(v.Locations||[]).join(', ');
  return '<tr><td><input class=ord size=2 value="'+(idx+1)+'" onchange="setLibOrder(\''+v.ItemId+'\',this.value)"></td>'+
  '<td><img src="/emby/Items/'+v.ItemId+'/Images/Primary?api_key='+T+'&v='+coverVer+'" style="width:96px;height:54px;object-fit:cover;background:#1b2735"></td>'+
  '<td>'+esc(v.Name)+'</td><td>'+esc(v.CollectionType)+'</td><td class=muted>'+esc(paths)+'</td><td>'+
  '<button onclick="editLib(\''+v.ItemId+'\')">编辑</button>'+
  '<button onclick="job(\'scan\',\'incremental\',\''+v.ItemId+'\')">扫描·增量</button>'+
  '<button onclick="job(\'scan\',\'full\',\''+v.ItemId+'\')">扫描·全量</button>'+
  '<button onclick="job(\'scrape\',\'incremental\',\''+v.ItemId+'\')">刮削·增量</button>'+
  '<button onclick="job(\'scrape\',\'full\',\''+v.ItemId+'\')">刮削·全量</button>'+
  '<button onclick="moveLib(\''+v.ItemId+'\',\'up\')" title="上移">↑</button>'+
  '<button onclick="moveLib(\''+v.ItemId+'\',\'down\')" title="下移">↓</button>'+
  '<button data-id="'+v.ItemId+'" data-name="'+esc(v.Name)+'" onclick="removeLib(this.dataset.id,this.dataset.name)" style="color:#ff9a92">移除</button></td></tr>'}).join('');
 }catch(e){toast('读取媒体库列表失败：'+(e&&e.message||e),false);}}
async function removeLib(id,name){
 if(!confirm('移除媒体库「'+name+'」？\n只移除库和数据库记录，磁盘上的媒体文件不会被删除。'))return;
 const r=await fetch('/emby/Library/VirtualFolders/'+id,{method:'DELETE',headers:{'X-Emby-Token':T}});
 if(r.ok){const d=await r.json().catch(()=>({}));toast('已移除媒体库（清理条目 '+(d.items||0)+' 个，文件未动）',true);loadLibs();}
 else{toast('移除失败：HTTP '+r.status,false);}
}
async function setLibOrder(id,pos){
 const n=parseInt(pos,10);
 if(!n||n<1){toast('序号必须是正整数',false);loadLibs();return;}
 const r=await fetch('/webui/api/libraries/'+id+'/order',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify({Position:n})});
 if(r.ok){toast('排序已更新',true);loadLibs();}else{toast('排序失败：HTTP '+r.status,false);loadLibs();}
}
async function moveLib(id,dir){
 const r=await fetch('/webui/api/libraries/'+id+'/order',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify({Direction:dir})});
 if(r.ok){loadLibs();}else{toast('移动失败：HTTP '+r.status,false);}
}
let lpAdd=[''];
function renderAddPaths(){
 $('lpathRows').innerHTML=lpAdd.map((p,i)=>{
  const browse=i===0?'<button onclick="openTree(\'add\',0)" title="选择目录">▾ 选择</button>':'';
  const rm=p?'<button onclick="removeAddPath('+i+')">移除</button>':'';
  return '<div style="margin:2px 0"><input size="46" value="'+esc(p)+'" placeholder="/media/电影" '+
   'oninput="onAddPathInput('+i+',this.value)"> '+browse+' '+rm+'</div>';
 }).join('');
}
function onAddPathInput(i,v){
 lpAdd[i]=v;
 if(i===lpAdd.length-1 && v.trim() && lpAdd.length<10) lpAdd.push('');
 renderAddPaths();
 $('lpathRows').querySelectorAll('input')[i].focus();
}
function removeAddPath(i){lpAdd.splice(i,1);if(!lpAdd.length)lpAdd=[''];renderAddPaths();}
async function addLib(){
 const paths=lpAdd.map(p=>p.trim()).filter(Boolean);
 const r=await fetch('/emby/Library/VirtualFolders',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},
  body:JSON.stringify({Name:$('lname').value,CollectionType:$('ltype').value,Paths:paths})});
 if(r.status===204){toast('媒体库已创建，正在扫描…',true);lpAdd=[''];renderAddPaths();loadLibs();}
 else{toast('创建失败：HTTP '+r.status,false);}
}
async function job(kind,mode,lib){const r=await fetch('/emby/Admin/Jobs',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},
 body:JSON.stringify({Kind:kind,Mode:mode,LibraryId:lib})});if(r.status===202){toast('任务已加入队列',true);loadJobs();}else{toast('提交失败：HTTP '+r.status,false);}}
async function loadJobs(){try{
 const raw=await api('/emby/Admin/Jobs')||{};
 const q=raw.Queued||raw.queued, dn=raw.Done||raw.done;
 const d={running:raw.Running||raw.running||null,queued:Array.isArray(q)?q:[],done:Array.isArray(dn)?dn:[]};
 const kindLabel=k=>({scan:'扫描',scrape:'刮削',rescan:'重新扫描',rescore:'重新刮削',index:'拼音索引'}[k]||k);
 const pct=j=>j.Total>0?Math.min(100,Math.round(j.Done/j.Total*100)):0;
 const bar=j=>j.Total>0?'<progress value="'+pct(j)+'" max="100"></progress>':'<progress></progress>';
 const line=j=>'<div style="margin:4px 0">'+bar(j)+' '+
   esc(kindLabel(j.Kind))+(j.Mode?'/'+esc(j.Mode==='full'?'全量':'增量'):'')+' · '+esc(j.LibraryName||'全部')+
   ' <span class=muted>'+(()=>{const n=j.Done||0;return j.Total>0?n+'/'+j.Total+' 条':(j.Kind==='scrape'?'已刮削 '+n+' 条':n+' 条')})()+' '+(j.Message||'')+'</span> '+
   '<button onclick="cancelJob(\''+j.Id+'\')">取消</button></div>';
 $('jobRunning').innerHTML=d.running?line(d.running):'<span class=muted>空闲</span>';
 $('jobQueued').innerHTML=(d.queued&&d.queued.length)?d.queued.map(j=>'<div class=muted>'+esc(kindLabel(j.Kind))+' · '+esc(j.LibraryName||'全部')+'（'+esc(j.Mode)+'） '+
   '<button onclick="cancelJob(\''+j.Id+'\')">取消</button></div>').join(''):'无';
 const stateLabel=j=>j.State==='failed'?'<span class=bad>失败</span>':(j.State==='canceled'?'<span class=muted>已取消</span>':'<span class=ok>完成</span>');
 const ts=s=>s?new Date(s*1000).toLocaleTimeString('zh-CN',{hour12:false}):'-';
 const dur=j=>(j.Finished&&j.Started)?((j.Finished-j.Started)+' 秒'):'-';
 $('jobDone').innerHTML=(d.done&&d.done.length)?d.done.slice(0,15).map(j=>'<div class=muted>'+stateLabel(j)+' '+esc(kindLabel(j.Kind))+' · '+esc(j.LibraryName||'全部')+' · '+j.Done+' 条 '+
   '<span class=muted>'+(j.Message?esc(j.Message)+' · ':'')+'开始 '+ts(j.Started)+' · 结束 '+ts(j.Finished)+' · 耗时 '+dur(j)+'</span> '+
   (j.Error?'<span class=bad>'+esc(j.Error)+'</span>':'')+'</div>').join(''):'无';
 }catch(e){toast('读取任务失败：'+(e&&e.message||e),false);}}
async function cancelJob(id){
 const r=await fetch('/emby/Admin/Jobs/'+id,{method:'DELETE',headers:{'X-Emby-Token':T}});
 if(r.ok){toast('已取消任务',true);loadJobs();}else{toast('取消失败：HTTP '+r.status,false);}
}
async function loadLogs(n){logLimit=n;try{
 const d=await api('/emby/Admin/Logs?limit='+n)||{};
 const lines=Array.isArray(d.Lines)?d.Lines:[];
 $('logs').textContent=lines.join('\n')||'—';$('logs').scrollTop=$('logs').scrollHeight;
 }catch(e){$('logs').textContent='读取日志失败：'+(e&&e.message||e);}}
function toggleAutoLog(){autoLog=$('autolog').checked;}
async function loadKeys(){try{
 const d=await api('/emby/Auth/Keys')||{};
 const items=Array.isArray(d.Items)?d.Items:[];
 if(!items.length){$('keystate').innerHTML='<span class=muted>还没有 API Key</span>';return}
 $('keystate').innerHTML=items.map(k=>'<div style="margin:4px 0"><code>'+esc(k.AccessToken)+'</code> '+
  '<button onclick="copyKey(\''+esc(k.AccessToken)+'\')">复制</button> '+
  '<span class=muted>'+esc(k.AppName||'')+'</span></div>').join('');
 }catch(e){$('keystate').innerHTML='<span class=bad>读取失败：'+esc(e&&e.message||e)+'</span>';}}
async function newKey(){const r=await fetch('/emby/Auth/Keys',{method:'POST',headers:{'X-Emby-Token':T}});
 const d=await r.json().catch(()=>({}));
 if(d.AccessToken){toast('已生成新 API Key',true);copyKey(d.AccessToken);}else{toast('生成失败：HTTP '+r.status,false)}loadKeys();}
async function copyKey(k){try{await navigator.clipboard.writeText(k);toast('已复制到剪贴板',true)}catch{toast('复制失败，请手动复制：'+k,false)}}
function closeModal(){$('modal').style.display='none';}
let lpEdit=null;
function editLib(id){
 const v=libsCache.find(x=>x.ItemId===id);if(!v)return;
 lpEdit={id:id,name:v.Name,type:v.CollectionType,paths:(v.Locations||[]).slice()};
 renderEditModal();
}
function renderEditModal(){
 $('modalBody').innerHTML='<h2>编辑媒体库</h2>'+
  '<div>名称 <input id="eName" size="16" value="'+esc(lpEdit.name)+'"></div>'+
  '<div style="margin-top:6px">类型 <select id="eType">'+
   '<option value="movies"'+(lpEdit.type==='movies'?' selected':'')+'>电影</option>'+
   '<option value="tvshows"'+(lpEdit.type==='tvshows'?' selected':'')+'>剧集</option></select></div>'+
  '<div style="margin-top:8px">路径<div id="ePathRows" style="margin-top:4px"></div></div>'+
  '<div style="margin-top:6px"><button onclick="lpEdit.paths.push(\'\');renderEditPaths()">+ 添加路径</button></div>'+
  '<div style="margin-top:10px">封面 <input type="file" id="eCover" accept="image/*"></div>'+
  '<div style="margin-top:12px"><button onclick="saveLib()">保存</button> <button onclick="closeModal()">取消</button></div>';
 renderEditPaths();
 $('modal').style.display='flex';
}
function renderEditPaths(){
 $('ePathRows').innerHTML=lpEdit.paths.map((p,i)=>{
  const rm=p?'<button onclick="removeEditPath('+i+')">移除</button>':'';
  return '<div style="margin:2px 0"><input size="44" value="'+esc(p)+'" placeholder="/media/剧集" '+
   'oninput="lpEdit.paths['+i+']=this.value;renderEditPaths()"> '+
   '<button onclick="openTree(\'edit\','+i+')">编辑</button> '+rm+'</div>';
 }).join('');
}
function removeEditPath(i){lpEdit.paths.splice(i,1);if(!lpEdit.paths.length)lpEdit.paths=[''];renderEditPaths();}
async function saveLib(){
 const body={Name:$('eName').value,CollectionType:$('eType').value,
  Paths:lpEdit.paths.map(p=>p.trim()).filter(Boolean)};
 const r1=await fetch('/emby/Library/VirtualFolders/'+lpEdit.id,{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify(body)});
 if(!r1.ok){toast('保存失败：HTTP '+r1.status,false);return}
 const f=$('eCover').files[0];
 if(f){const r2=await fetch('/emby/Library/VirtualFolders/'+lpEdit.id+'/Image',{method:'POST',headers:{'X-Emby-Token':T},body:f});
  if(!r2.ok){toast('封面上传失败：HTTP '+r2.status,false);return}}
 coverVer=Date.now();
 toast(f?'已保存，封面已更新':'已保存',true);
 closeModal();loadLibs();
}

// ---- 目录树（按需加载，勾选状态跨层级保留）----
let treeState=null, treeSel=new Set(), treePath='';
async function openTree(mode,index){
 treeState={mode:mode,index:index};treeSel=new Set();
 const start=(mode==='add'?lpAdd[index]:lpEdit.paths[index])||'';
 await treeGo(start);
 $('modal').style.display='flex';
}
async function treeGo(path){
 const d=await api('/emby/Admin/Fs?path='+encodeURIComponent(path));
 if(!d||!d.Dirs){toast('无法读取目录',false);return;}
 treePath=d.Path;
 const rows=d.Dirs.map(x=>{
  const cb=treeSel.has(x.Path);
  return '<div style="margin:3px 0"><input type="checkbox" data-p="'+esc(x.Path)+'"'+(cb?' checked':'')+'> '+
   '<a href="#" data-dir="'+esc(x.Path)+'">'+esc(x.Name)+'</a></div>';
 }).join('');
 $('modalBody').innerHTML='<h2>选择目录</h2>'+
  '<div class=muted style="word-break:break-all">当前：'+esc(d.Path)+'</div>'+
  '<div style="margin:8px 0"><button data-back>'+'← 返回上层</button> '+
  '<button data-here>勾选当前目录</button></div>'+
  '<div style="max-height:46vh;overflow:auto;border:1px solid #243040;padding:8px">'+
  (rows||'<span class=muted>没有子目录</span>')+'</div>'+
  '<div style="margin-top:10px"><button data-ok>确认</button> <button data-cancel>取消</button></div>';
 const back=$('modalBody').querySelector('[data-back]');
 back.disabled=!d.Parent;
 if(d.Parent) back.onclick=()=>treeGo(d.Parent);
 $('modalBody').querySelector('[data-here]').onclick=()=>{treeSel.add(treePath);treeGo(treePath);};
 $('modalBody').querySelectorAll('input[type=checkbox]').forEach(cb=>{
  cb.onchange=()=>{cb.checked?treeSel.add(cb.dataset.p):treeSel.delete(cb.dataset.p);};
 });
 $('modalBody').querySelectorAll('[data-dir]').forEach(a=>{
  a.onclick=e=>{e.preventDefault();treeGo(a.dataset.dir);};
 });
 $('modalBody').querySelector('[data-ok]').onclick=treeConfirm;
 $('modalBody').querySelector('[data-cancel]').onclick=()=>{
  if(treeState&&treeState.mode==='edit')renderEditModal();else closeModal();
 };
}
function treeConfirm(){
 const sel=[...treeSel];
 if(treeState.mode==='add'){
  lpAdd=sel.length?sel.slice():[treePath];
  renderAddPaths();
 }else{
  lpEdit.paths[treeState.index]=sel[0]||treePath;
  renderEditModal();
  return;
 }
 closeModal();
}
async function loadUsers(){try{
 loadPublicUsers();
 const rawUsers=await api('/emby/Users');
 const d=Array.isArray(rawUsers)?rawUsers:[];
 const rawLibs=libsCache.length?libsCache:await api('/emby/Library/VirtualFolders');
 const libs=Array.isArray(rawLibs)?rawLibs:[];
 $('users').querySelector('tbody').innerHTML=d.map(u=>{const p=u.Policy||{};const all=p.EnableAllFolders!==false;
  const acc=all?'全部':(p.EnabledFolders||[]).map(id=>{const x=libs.find(l=>l.ItemId===id);return x?x.Name:id}).join('、');
  const av = u.PrimaryImageTag
    ? '<img src="/emby/Users/'+u.Id+'/Images/Primary?api_key='+T+'&v='+encodeURIComponent(u.PrimaryImageTag)+'" class=avatar>'
    : '<span class="avatar avatar-empty">?</span>';
  return '<tr><td>'+av+'</td><td>'+esc(u.Name)+'</td><td>'+(p.IsAdministrator?'是':'-')+'</td><td>'+(p.IsDisabled?'<span class=bad>是</span>':'-')+
  '</td><td class=muted>'+esc(acc)+'</td><td>'+
  '<button data-id="'+u.Id+'" data-name="'+esc(u.Name)+'" onclick="rename(this.dataset.id,this.dataset.name)">改名</button>'+
  '<button onclick="chpass(\''+u.Id+'\')">改密</button>'+
  '<button onclick="toggleDisable(\''+u.Id+'\','+(!p.IsDisabled)+')">'+(p.IsDisabled?'启用':'禁用')+'</button>'+
  '<button onclick="setLibs(\''+u.Id+'\')">库权限</button>'+
  '<button onclick="openAvatar(\''+u.Id+'\')">头像</button>'+
  '<button data-id="'+u.Id+'" data-name="'+esc(u.Name)+'" onclick="removeUser(this.dataset.id,this.dataset.name)" style="color:#ff9a92">删除</button></td></tr>'}).join('');
 }catch(e){toast('读取用户失败：'+(e&&e.message||e),false);}}
async function loadPublicUsers(){
 try{
  const d=await api('/webui/api/public-users');
  if($('pubUsers')) $('pubUsers').checked = !d || d.Visible !== false;
 }catch(_){}
}
async function savePublicUsers(){
 try{
  const r=await fetch('/webui/api/public-users',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify({Visible:!!$('pubUsers').checked})});
  if(r.ok){toast('已保存登录页设置',true);}else{toast('保存失败：HTTP '+r.status,false);}
 }catch(e){toast('保存失败：'+(e&&e.message||e),false);}
}
async function addUser(){
 const name=$('uname').value.trim(), pass=$('upass').value;
 if(!name){toast('用户名不能为空',false);return;}
 const r=await fetch('/emby/Users/New',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},
   body:JSON.stringify({Name:name,Password:pass,IsAdministrator:$('uadmin').checked})});
 if(r.ok){
   // 表单重置：清空输入并归还焦点，避免残留上一次的用户名/密码。
   $('uname').value=''; $('upass').value=''; $('uadmin').checked=false;
   $('uname').focus();
   toast('用户已创建',true); loadUsers();
 }else{toast('创建失败：HTTP '+r.status,false);}
}
async function removeUser(id,name){
 if(!confirm('删除账号「'+name+'」？\n只删除账号与令牌，不会删除媒体文件。'))return;
 try{
  const r=await fetch('/emby/Users/'+id,{method:'DELETE',headers:{'X-Emby-Token':T}});
  if(r.status===204){toast('账号已删除',true);loadUsers();}
  else{toast('删除失败：HTTP '+r.status,false);}
 }catch(e){toast('删除失败：'+(e&&e.message||e),false);}
}
async function rename(id,old){const n=prompt('新名称',old);if(!n)return;
 const r=await fetch('/emby/Users/'+id,{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify({Name:n})});
 if(r.ok){toast('已改名',true);loadUsers();}else{toast('改名失败：HTTP '+r.status,false);}}
async function chpass(id){const pw=prompt('新密码');if(!pw)return;
 const r=await fetch('/emby/Users/'+id+'/Password',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify({NewPw:pw})});
 if(r.status===204){toast('密码已修改',true)}else{toast('修改失败：HTTP '+r.status,false)}}
async function toggleDisable(id,disable){const r=await fetch('/emby/Users/'+id+'/Policy',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},
 body:JSON.stringify({IsDisabled:disable})});
 if(r.ok){toast(disable?'已禁用':'已启用',true);loadUsers();}else{toast('操作失败：HTTP '+r.status,false)}}
let avatarState=null;
function openAvatar(id){
 $('modalBody').innerHTML='<h2>设置头像</h2>'+
  '<div class=muted>选择图片后可拖动调整位置，用 +/- 缩放；头像会按圆形裁剪。</div>'+
  '<div style="margin:8px 0"><input type="file" id="avFile" accept="image/*"></div>'+
  '<div style="display:flex;gap:12px;align-items:flex-start">'+
   '<canvas id="avCanvas" width="512" height="512" style="width:300px;height:300px"></canvas>'+
   '<div><button onclick="avatarZoom(0.15)">＋</button><br><button onclick="avatarZoom(-0.15)">－</button><br>'+
   '<button onclick="avatarReset()">重置</button></div>'+
  '</div>'+
  '<div style="margin-top:12px"><button onclick="saveAvatar(\''+id+'\')">保存</button> <button onclick="closeModal()">取消</button></div>';
 $('modal').style.display='flex';
 avatarState={id:id,img:null,scale:1,ox:0,oy:0};
 const cv=$('avCanvas'), ctx=cv.getContext('2d');
 $('avFile').onchange=e=>{
  const f=e.target.files[0]; if(!f)return;
  const img=new Image(); img.onload=()=>{avatarState.img=img;avatarReset();}; img.src=URL.createObjectURL(f);
 };
 let dragging=false,lx=0,ly=0;
 cv.onpointerdown=e=>{dragging=true;lx=e.clientX;ly=e.clientY;cv.setPointerCapture(e.pointerId);};
 cv.onpointermove=e=>{
  if(!dragging||!avatarState.img)return;
  const k=512/cv.clientWidth;
  avatarState.ox+=(e.clientX-lx)*k; avatarState.oy+=(e.clientY-ly)*k;
  lx=e.clientX; ly=e.clientY; drawAvatar(ctx);
 };
 cv.onpointerup=()=>{dragging=false;};
 drawAvatar(ctx);
}
function avatarZoom(d){ if(!avatarState)return; avatarState.scale=Math.max(0.3,Math.min(4,avatarState.scale+d)); drawAvatar($('avCanvas').getContext('2d')); }
function avatarReset(){ if(!avatarState)return; avatarState.scale=1; avatarState.ox=0; avatarState.oy=0; drawAvatar($('avCanvas').getContext('2d')); }
function drawAvatar(ctx){
 const cv=$('avCanvas'); if(!ctx||!cv)return;
 ctx.clearRect(0,0,512,512);
 if(!avatarState||!avatarState.img)return;
 const img=avatarState.img;
 const base=Math.max(512/img.width,512/img.height)*avatarState.scale;
 const w=img.width*base,h=img.height*base;
 const x=(512-w)/2+avatarState.ox, y=(512-h)/2+avatarState.oy;
 ctx.save(); ctx.beginPath(); ctx.arc(256,256,256,0,Math.PI*2); ctx.clip();
 ctx.drawImage(img,x,y,w,h); ctx.restore();
}
async function saveAvatar(id){
 if(!avatarState||!avatarState.img){toast('请先选择图片',false);return;}
 const cv=$('avCanvas');
 cv.toBlob(async blob=>{
  if(!blob){toast('生成图片失败',false);return;}
  const r=await fetch('/emby/Users/'+id+'/Images/Primary',{method:'POST',headers:{'X-Emby-Token':T},body:blob});
  if(r.ok||r.status===204){toast('头像已更新',true);closeModal();loadUsers();}
  else{toast('上传失败：HTTP '+r.status,false);}
 },'image/jpeg',0.9);
}
async function setLibs(id){try{
 const rl=await api('/emby/Library/VirtualFolders');
 const libs=Array.isArray(rl)?rl:[];
 const ru=await api('/emby/Users');
 const users=Array.isArray(ru)?ru:[];
 const u=users.find(x=>x.Id===id)||{Name:'',Policy:{}};
 const p=u.Policy||{};
 const all=p.EnableAllFolders!==false;
 const selected=new Set((p.EnabledFolders||[]).filter(Boolean));
 const rows=libs.map(l=>'<label style="display:block;margin:6px 0"><input type="checkbox" class="libbox" value="'+esc(l.ItemId)+'"'+
   ((all||selected.has(l.ItemId))?' checked':'')+'> '+esc(l.Name)+' <span class=muted>('+esc(l.CollectionType||'')+')</span></label>').join('');
 $('modalBody').innerHTML='<h2>库权限 · '+esc(u.Name)+'</h2>'+
   '<label style="display:block;margin-bottom:8px;padding-bottom:6px;border-bottom:1px solid #243040">'+
   '<input type="checkbox" id="libAll"'+(all?' checked':'')+'> <b>全选</b></label>'+
   '<div id="libList">'+rows+'</div>'+
   '<div style="margin-top:12px"><button onclick="saveLibs(\''+id+'\')">保存</button> <button onclick="closeModal()">取消</button></div>';
 $('libAll').onchange=()=>{document.querySelectorAll('.libbox').forEach(b=>b.checked=$('libAll').checked);};
 document.querySelectorAll('.libbox').forEach(b=>{b.onchange=()=>{
   $('libAll').checked=[...document.querySelectorAll('.libbox')].every(x=>x.checked);
 };});
 $('modal').style.display='flex';
 }catch(e){toast('读取库权限失败：'+(e&&e.message||e),false);}}
async function saveLibs(id){
 const boxes=[...document.querySelectorAll('.libbox')];
 const ids=boxes.filter(b=>b.checked).map(b=>b.value);
 const all=ids.length===boxes.length && boxes.length>0;
 const body={EnableAllFolders:all,EnabledFolders:all?[]:ids};
 const r=await fetch('/emby/Users/'+id+'/Policy',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify(body)});
 if(r.ok){closeModal();toast('库权限已保存',true);loadUsers();}else{toast('保存失败：HTTP '+r.status,false);}
}
async function loadNameIndex(){
 try{
  const d=await api('/webui/api/name-index')||{};
  if($('setIndexTime'))$('setIndexTime').value=d.Time||'';
  if($('setIndexHint'))$('setIndexHint').textContent=d.LastRun?('上次完成：'+d.LastRun):'尚未全量重建';
 }catch(e){if($('setIndexHint'))$('setIndexHint').textContent='读取失败';}
}
async function saveNameIndexTime(){
 const t=($('setIndexTime').value||'').trim();
 const r=await fetch('/webui/api/name-index',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify({Time:t})});
 if(r.ok){toast(t?'拼音索引定时重建已保存':'已关闭拼音索引定时重建',true);loadNameIndex();}
 else {let x='';try{x=(await r.json()).error||''}catch(_){}toast('保存失败：HTTP '+r.status+(x?' '+x:''),false);}
}
async function rebuildNameIndex(){
 const r=await fetch('/webui/api/name-index/rebuild',{method:'POST',headers:{'X-Emby-Token':T}});
 if(r.status===202){toast('已加入拼音索引重建任务',true);loadJobs();loadNameIndex();}
 else{toast('提交失败：HTTP '+r.status,false);}
}
async function loadSettings(){
 try{
  const d=await api('/webui/api/settings')||{};
  $('setTmdbLang').value=d.TMDBLang||'zh-CN';
  $('setProxy').value=d.Proxy||'';
  $('setNoProxy').value=d.NoProxy||'';
  $('setRate').value=d.ScrapeRate||15;
  $('setAutoScrape').checked=d.AutoScrape!==false;
  $('setNFO').checked=!!d.NFOWrite;
  $('setTmdbKeyState').textContent=d.TMDBKeySet?'已配置（留空不修改）':'未配置';
  $('setTmdbKey').value='';
  $('setClearKey').checked=false;
  loadNameIndex();
 }catch(e){toast('读取设置失败：'+(e&&e.message||e),false);}
}
async function saveSettings(){
 const body={TMDBAPIKey:$('setTmdbKey').value,ClearTMDBKey:$('setClearKey').checked,
  TMDBLang:$('setTmdbLang').value.trim()||'zh-CN',Proxy:$('setProxy').value.trim(),
  NoProxy:$('setNoProxy').value.trim(),ScrapeRate:parseInt($('setRate').value||'15',10),
  AutoScrape:$('setAutoScrape').checked,NFOWrite:$('setNFO').checked};
 try{
  const r=await fetch('/webui/api/settings',{method:'POST',headers:{'Content-Type':'application/json','X-Emby-Token':T},body:JSON.stringify(body)});
  if(r.ok){$('setTmdbKey').value='';$('setClearKey').checked=false;toast('设置已保存，新任务生效',true);loadSettings();}
  else {let x='';try{x=(await r.json()).error||''}catch(_){} toast('保存失败：HTTP '+r.status+(x?' '+x:''),false);}
 }catch(e){toast('保存失败：'+(e&&e.message||e),false);}
}
setInterval(async()=>{
 if(!T||pollInFlight)return; // 上一次请求还没回来就跳过，避免请求堆积
 const needJobs=$('s-jobs').style.display!=='none';
 const needLogs=autoLog&&$('s-logs').style.display!=='none';
 if(!needJobs&&!needLogs)return;
 pollInFlight=true;
 try{if(needJobs)await loadJobs();if(needLogs)await loadLogs(logLimit);}
 catch(_){}
 finally{pollInFlight=false;}
},2000);
renderAddPaths();

// 启动：只跑一次（startUI 自带 initialized 防重）。
(async function boot(){
 if(!T){showLogin('');return;} // 无令牌：直接展示登录框，不发任何请求
 try{
  const me=await api('/emby/Users/Me');
  if(!me||!me.IsAdmin){showLogin('登录已失效，请重新登录');return;}
  startUI();
 }catch(e){
  // 401/403 已由 api() 统一 showLogin；其它错误（网络/超时）也退回登录框，别卡在空白页。
  if(T)showLogin('无法连接服务器，请重新登录');
 }
})();
</script></body></html>`
