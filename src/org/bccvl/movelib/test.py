from org.bccvl.movelib import move
from org.bccvl.tasks.utils import AuthTkt

swift_source = { 'url': 'swift://nectar/container2/test/test2.txt',
           'auth': 'https://keystone.rc.nectar.org.au:5000/v2.0/',
           'user': 'username@griffith.edu.au',
	         'key' : 'password',
           'os_tenant_name': 'pt-12345',
	         'auth_version': '2'   
}

swift_dest = { 'url': 'swift://nectar/container2',
         'auth': 'https://keystone.rc.nectar.org.au:5000/v2.0/',
         'user': 'username@griffith.edu.au',
	       'key' : 'password',
         'os_tenant_name': 'pt-12345',
	       'auth_version': '2'   
}

# swift destination with filename specified
swift_dest2 = { 'url': 'swift://nectar/container2',
         'auth': 'https://keystone.rc.nectar.org.au:5000/v2.0/',
         'user': 'username@griffith.edu.au',
	       'key' : 'password',
         'os_tenant_name': 'pt-12345',
	       'auth_version': '2',
	 'filename': 'test3.txt'   
}

# Generate cookies for http
ticket = AuthTkt('ibycgtpw', 'admin')
cookies = {'name': '__ac', 'value': ticket.ticket(), 'domain': '', 'path': '/', 'secure': True}
http_source = { 'url': 'http://www.news.com.au/national/breaking-news/i-wasnt-moonlighting-as-nurse-qld-mp/story-e6frfku9-1227604589349', 
                'cookies': cookies}

scp_source = { 'url': 'scp://username:password@hostname/srcpath/srcfile1'}
scp_dest = { 'url': 'scp://username:password@hostname/destpath', 'filename': 'destfilename.txt'}
ala_source = { 'url': 'ala://ala/?lsid=urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae' }
file_source = { 'url': 'file:///home/plone/bccvl_buildout/src/org.bccvl.movelib/src/README.md'}
file_dest = { 'url': 'file:///home/plone/bccvl_buildout/src/org.bccvl.movelib/src', 'filename': 't1.md'}
file_dest2 = { 'url': 'file:///home/plone/bccvl_buildout/src/org.bccvl.movelib/src'}


move(swift_source, swift_dest2)
move(swift_source, swift_dest)
move(http_source, swift_dest)
move(scp_source, scp_dest)
move(ala_source, scp_dest)
move(ala_source, swift_dest)
move(scp_source, swift_dest)
move(file_source, file_dest)
move(scp_source, file_dest2)
move(http_source, file_dest2)
move(swift_source, file_dest2)

