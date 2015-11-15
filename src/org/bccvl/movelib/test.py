from org.bccvl.movelib import move

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

http_source = { 'url': 'http://www.news.com.au/national/breaking-news/i-wasnt-moonlighting-as-nurse-qld-mp/story-e6frfku9-1227604589349'
}

scp_source = { 'url': 'scp://root:ibycgtpw@192.168.100.200/root/getusage.py'}
scp_dest = { 'url': 'scp://root:ibycgtpw@192.168.100.200/root', 'filename': 't34.txt'}

ala_source = { 'url': 'ala://ala/?lsid=urn:lsid:biodiversity.org.au:afd.taxon:31a9b8b8-4e8f-4343-a15f-2ed24e0bf1ae' }


move(swift_source, swift_dest2)
move(swift_source, swift_dest)
move(http_source, swift_dest)
move(scp_source, scp_dest)
move(ala_source, scp_dest)
move(ala_source, swift_dest)

