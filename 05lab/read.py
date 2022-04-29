from snakebite.client import Client


cli = Client('localhost',9000)
text = []

for line in cli.text(['/books/detective_stories/opowiesc.txt']):
	text = str(line).split('\\n')[:5]

with open('intro.txt', 'w') as f:
	f.write(str(text))
