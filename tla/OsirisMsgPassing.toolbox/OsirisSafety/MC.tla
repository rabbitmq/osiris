---- MODULE MC ----
EXTENDS OsirisMsgPassing, TLC

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
r1, r2, r3
----

\* MV CONSTANT declarations@modelParameterConstants
CONSTANTS
a, b
----

\* MV CONSTANT definitions R
const_158037920971472000 == 
{r1, r2, r3}
----

\* MV CONSTANT definitions V
const_158037920971473000 == 
{a, b}
----

\* SYMMETRY definition
symm_158037920971474000 == 
Permutations(const_158037920971473000)
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_158037920971475000 ==
/\ coord_epoch < 3
/\ start_stop_ctr < 3
----
=============================================================================
\* Modification History
\* Created Thu Jan 30 02:13:29 PST 2020 by jack
