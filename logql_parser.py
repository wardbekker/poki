# this lex/parser can parse a loki query in the format of

# {cluster="autopilot_eu", application="nginx"} |= "sample logline" 
import ply.lex as lex
import ply.yacc as yacc

# List of token names. This is always required
tokens = (
    'LBRACE',
    'RBRACE',
    'EQUALS',
    'PIPE_EXACT',
    'STRING',
    'COMMA',
    'STREAM_LABEL'
)

# Regular expression rules for simple tokens
t_LBRACE = r'\{'
t_RBRACE = r'\}'
t_EQUALS = r'='
t_PIPE_EXACT = r'\|='
t_COMMA = r','

# A string containing ignored characters (spaces and tabs)
t_ignore = ' \t'

# Define a rule so we can track line numbers
def t_newline(t):
    r'\n+'
    t.lexer.lineno += len(t.value)

# Define a rule for strings
def t_STRING(t):
    r'"([^"]*)"'
    t.value = t.value[1:-1]
    return t

def t_STREAM_LABEL(t):
    r'[a-zA-Z_][a-zA-Z0-9_]*'
    return t

# Error handling rule
def t_error(t):
    print("Illegal character '%s'" % t.value[0])
    t.lexer.skip(1)

# Define the grammar
def p_expression_query(p):
    '''expression : LBRACE criteria RBRACE PIPE_EXACT STRING'''
    p[0] = {
        'type': 'query',
        'criteria': p[2],
        'exact_string_search': p[5]
    }

def p_criteria_list(p):
    '''criteria : criteria COMMA criteria'''
    p[0] = p[1] + p[3]

def p_criteria_item(p):
    '''criteria : STREAM_LABEL EQUALS STRING'''
    p[0] = [(p[1], p[3])]

# Error rule for syntax errors
def p_error(p):
    print("Syntax error in input!")

# Build the lexer
lexer = lex.lex()
parser = yacc.yacc()

def parse_logql(query):
    return parser.parse(query)

