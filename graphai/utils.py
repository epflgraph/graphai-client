from os import system
from datetime import datetime
from termcolor import cprint


graph_ai_server = 'http://127.0.0.1:28800'  # port-forward to graphai-test

TerminalColors = {
	None			: '0',
	'grey'			: '0',
	'black'			: '0;30',
	'red'			: '0;31',
	'green'			: '0;32',
	'orange'		: '0;33',
	'blue'			: '0;34',
	'magenta'		: '0;35',
	'cyan'			: '0;36',
	'light gray'	: '0;37',
	'dark gray'		: '1;30',
	'light red'		: '1;31',
	'light green'	: '1;32',
	'yellow'		: '1;33',
	'light purple'	: '1;35',
	'light cyan'	: '1;36',
	'white'			: '1;37'
}

def StatusMSG(Message, Color=None, Sections=(), PrintFlag=True, UseTerminal=False):
	"""
	Print a nice status message.

	:param Message: message to print.
	:type Message: str
	:param Color: color of the message. If None, the default color is used. Available colors are:

		- 'grey', 'black', 'red', 'green', 'orange', 'blue', 'magenta', 'cyan', 'light gray', 'dark gray', 'light red',
			'light green', 'yellow', 'light purple', 'light cyan' and 'white' in terminal mode.
		- 'grey', 'red', 'green', 'yellow', 'blue', 'magenta', 'cyan' and 'white' in non-terminal mode.

	:type Color: str, None
	:param Sections: list of strings representing the sections which will be displayed at the beginning of the message.
	:type Sections: list
	:param PrintFlag: If False nothing is printed.
	:type PrintFlag: bool
	:param UseTerminal: should the terminal mode be used.
	:type UseTerminal: bool
	"""
	if not PrintFlag:
		return
	GlobalString = '[%s] ' % f"{datetime.now():%Y-%m-%d %H:%M}"
	for section in Sections:
		GlobalString += '[%s] ' % section
	GlobalString += Message
	if UseTerminal:
		GlobalString = GlobalString.replace('"', '\\"')
		ColorCode = '\033[%sm' % TerminalColors[Color]
		system('COLOR=\'%s\'; NC=\'\033[0m\'; printf "${COLOR}%s${NC}\n"' % (ColorCode, GlobalString))
	else:
		cprint(GlobalString, Color)


