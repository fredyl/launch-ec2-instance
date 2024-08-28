An error occurred: 
[PARSE_SYNTAX_ERROR] Syntax error at or near '.'. SQLSTATE: 42601 (line 4, pos 102)

== SQL ==

                    MERGE INTO groups AS target
                    USING temp_view AS source
                    ON target.d = source.d AND target.e = source.e AND target.v = source.v AND target.. = source.. AND target.b = source.b AND target.r = source.r AND target.o = source.o AND target.n = source.n AND target.z = source.z AND target.e = source.e AND target.. = source.. AND target.p = source.p AND target.b = source.b AND target.i = source.i AND target._ = source._ AND target.g = source.g AND target.r = source.r AND target.o = source.o AND target.u = source.u AND target.p = source.p AND target.s = source.s
------------------------------------------------------------------------------------------------------^^^
                    WHEN MATCHED THEN
                        UPDATE SET *
                    WHEN NOT MATCHED THEN
                        INSERT *
