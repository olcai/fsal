NAME=fsal
REBAR=./rebar
ERL_COMMAND=ERL_LIBS=deps erl -pa ebin include -name $(NAME)@127.0.0.1

default: compile

deps:
	$(REBAR) get-deps

compile: deps
	$(REBAR) compile

test:
	$(REBAR) eunit skip_deps=true

clean:
	$(REBAR) clean

run:
	$(ERL_COMMAND) -s $(NAME)

.PHONY: deps test
