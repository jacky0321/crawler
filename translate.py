from urllib.request import Request, urlopen, ProxyHandler, build_opener, install_opener
from urllib.parse import urlencode
import json
from random import choice
from lxml import etree

ip = ['183.131.215.86:8080', '58.23.130.18:8080', '117.143.109.170:80']

proxy_support = ProxyHandler({'http':choice(ip)})
opener = build_opener(proxy_support)
opener.addheaders = [('User-Agent', 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:53.0) Gecko/20100101 Firefox/53.0')]
install_opener(opener)

#url = 'http://fanyi.youdao.com/translate_o?smartresult=dict&smartresult=rule&sessionFrom=https://www.baidu.com/link'
url = 'https://www.whatismyip.com'

head = {}
head['User-Agent'] = 'Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:53.0) Gecko/20100101 Firefox/53.0'

req = Request(url)
response = urlopen(req)
print(response.read())
t = response.read().lower().decode('utf-8')
html = etree.HTML(t)
print(html)
#print(html.xpath(u"//*[@class='ip']")[0].text)




'''data = {}
content = input('enter a string:')
data['i'] = content
data['from'] = 'AUTO'
data['to'] = 'AUTO'
data['smartresult'] = 'dict'
data['client'] = 'fanyideskweb'
data['salt'] = '1498539915150'
data['sign'] = 'fb49ef0d1358d8564015df6b1e124241'
data['doctype'] = 'json'
data['version'] = '2.1'
data['keyfrom'] = 'fanyi.web'
data['action'] = 'FY_BY_CLICKBUTTON'
data['typoResult'] = 'true'

data = urlencode(data).encode('utf-8')

req = Request(url, data, head)
response = urlopen(req)
html = response.read().decode('utf-8')

print(html)
target = json.loads(html)
print(target)
#print(target['smartResult']['entries'][1].strip())
'''
