from snakebite.client import Client


cli = Client('localhost', 9000)

for f in cli.copyToLocal(['/books_read/opowiesc.txt'], '/home'):
	print(f)
