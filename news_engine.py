import asyncio,re,html,urllib.parse,aiohttp
from bs4 import BeautifulSoup,SoupStrainer
from datetime import datetime,timedelta
from typing import List,Dict,Tuple

USER_AGENT="Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36"
MAX_CONCURRENT_REQUESTS,REQUEST_TIMEOUT,MAX_CONTENT_LENGTH,DEFAULT_DAYS_AGO=10,15,1000000,7

MONTH_TRANSLATIONS={'يناير':'January','فبراير':'February','مارس':'March','أبريل':'April','مايو':'May','يونيو':'June','يوليو':'July','أغسطس':'August','سبتمبر':'September','أكتوبر':'October','نوفمبر':'November','ديسمبر':'December','january':'January','february':'February','march':'March','april':'April','may':'May','june':'June','july':'July','august':'August','september':'September','october':'October','november':'November','december':'December','jan':'January','feb':'February','mar':'March','apr':'April','jun':'June','jul':'July','aug':'August','sep':'September','oct':'October','nov':'November','dec':'December'}
ARABIC_INDIC_DIGITS={'٠':'0','١':'1','٢':'2','٣':'3','٤':'4','٥':'5','٦':'6','٧':'7','٨':'8','٩':'9'}

SEARCH_PROVIDERS={"Bing":{"url":"https://www.bing.com/news/search?q={query}&qft=interval%3d\"{days}d\"","selector":"a.title, a.news-item-heading, h2 a, a[href*='http']","date_patterns":[r'(\d{1,2})\s+([أ-يa-zA-Z]+)\s+(20\d{2})',r'(20\d{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12]\d|3[01])']},"Google":{"url":"https://www.google.com/search?q={query}&tbm=nws&tbs=qdr:d{days}","selector":"a[href*='/url?q='], a[aria-label]","date_patterns":[r'(\d{1,2})\s+([أ-يa-zA-Z]+)\s+(20\d{2})',r'(20\d{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12]\d|3[01])']}}

QUERY_ALIASES={
"الصين":["الصين","China","الجمهورية الشعبية","بكين","Beijing","الحزب الشيوعي الصيني"],
"أمريكا":["أمريكا","الولايات المتحدة","USA","United States","واشنطن","Washington"],
"روسيا":["روسيا","Russia","موسكو","Moscow","الكرملين","Kremlin"],
"إيران":["إيران","Iran","طهران","Tehran"],
"تركيا":["تركيا","Turkey","أنقرة","Ankara"],
"السعودية":["السعودية","KSA","Riyadh","الرياض"],
"مصر":["مصر","Egypt","القاهرة","Cairo"],
"الإمارات":["الإمارات","UAE","أبوظبي","Abu Dhabi","دبي","Dubai"],
"إسرائيل":["إسرائيل","Israel","تل أبيب","Tel Aviv"],
"النفط":["النفط","أسعار النفط","برنت","OPEC","أوبك","Crude Oil"],
"الذهب":["الذهب","Gold","أسعار الذهب"],
"الفائدة":["سعر الفائدة","الفيدرالي","Federal Reserve","البنك المركزي","Interest Rates"],
"التضخم":["التضخم","Inflation","مؤشر أسعار المستهلكين","CPI"]
}

def clean_text(t:str)->str:
    if not t:return ""
    t=html.unescape(t)
    for a,b in ARABIC_INDIC_DIGITS.items():t=t.replace(a,b)
    return re.sub(r'\s+',' ',t).strip()

def parse_date(d_str:str)->datetime:
    if not d_str:return None
    d_str=clean_text(d_str)
    for ar,en in MONTH_TRANSLATIONS.items():
        if ar in d_str.lower():d_str=re.sub(re.escape(ar),en,d_str,flags=re.IGNORECASE)
    for fmt in ('%d %B %Y','%Y-%m-%d','%d/%m/%Y','%Y/%m/%d','%b %d, %Y','%d %b %Y'):
        try:return datetime.strptime(d_str,fmt)
        except:pass
    m=re.search(r'(\d{1,2})\s+([A-Za-z]+)\s+(20\d{2})',d_str)
    if m:
        try:return datetime.strptime(f"{m.group(1)} {m.group(2)} {m.group(3)}",'%d %B %Y')
        except:pass
    m=re.search(r'(20\d{2})[-/](0?[1-9]|1[0-2])[-/](0?[1-9]|[12]\d|3[01])',d_str)
    if m:
        try:return datetime(int(m.group(1)),int(m.group(2)),int(m.group(3)))
        except:pass
    return None

def extract_links(html_content:str,provider:str,base_url:str)->List[Dict]:
    results,pdata=[],SEARCH_PROVIDERS.get(provider,{})
    soup=BeautifulSoup(html_content,'html.parser',parse_only=SoupStrainer('a'))
    for tag in soup.find_all('a',href=True):
        href,title=tag['href'],clean_text(tag.get_text())
        if href.startswith('/url?q='):href=urllib.parse.parse_qs(urllib.parse.urlparse(href).query).get('q',[''])[0]
        elif href.startswith('/'):href=urllib.parse.urljoin(base_url,href)
        if not href.startswith('http') or any(x in href for x in ['google.com','bing.com','youtube.com','facebook.com']):continue
        context=clean_text(tag.parent.get_text() if tag.parent else title)
        dt=None
        for pat in pdata.get('date_patterns',[]):
            m=re.search(pat,context)
            if m:
                dt=parse_date(m.group(0))
                if dt:break
        results.append({'title':title or href,'url':href,'date':dt,'provider':provider})
    return results

async def fetch_provider(session:aiohttp.ClientSession,provider:str,query:str,days:int,sem:asyncio.Semaphore)->List[Dict]:
    async with sem:
        pconfig=SEARCH_PROVIDERS.get(provider)
        if not pconfig:return []
        url=pconfig['url'].format(query=urllib.parse.quote(query),days=days)
        try:
            async with session.get(url,headers={'User-Agent':USER_AGENT},timeout=REQUEST_TIMEOUT) as resp:
                if resp.status==200:
                    text=await resp.text()
                    return extract_links(text[:MAX_CONTENT_LENGTH],provider,url)
        except Exception:pass
        return []

async def collect_news(keywords:List[str],days:int=DEFAULT_DAYS_AGO)->List[Dict]:
    sem=asyncio.Semaphore(MAX_CONCURRENT_REQUESTS)
    expanded_queries=set()
    for kw in keywords:
        expanded_queries.add(kw)
        for main_key,aliases in QUERY_ALIASES.items():
            if kw.lower() in [main_key.lower()]+[a.lower() for a in aliases]:
                expanded_queries.update(aliases)
    async with aiohttp.ClientSession() as session:
        tasks=[fetch_provider(session,prov,q,days,sem) for prov in SEARCH_PROVIDERS for q in expanded_queries]
        res=await asyncio.gather(*tasks,return_exceptions=True)
    all_articles,seen=[],set()
    min_date=datetime.now()-timedelta(days=days)
    for r in res:
        if isinstance(r,list):
            for item in r:
                if item['url'] not in seen:
                    seen.add(item['url'])
                    if not item['date'] or item['date']>=min_date:
                        all_articles.append(item)
    return sorted(all_articles,key=lambda x:x['date'] or datetime.min,reverse=True)
