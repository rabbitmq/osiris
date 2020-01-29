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
const_1580311056580312000 == 
{r1, r2, r3}
----

\* MV CONSTANT definitions V
const_1580311056580313000 == 
{a, b}
----

\* SYMMETRY definition
symm_1580311056580314000 == 
Permutations(const_1580311056580313000)
----

\* CONSTRAINT definition @modelParameterContraint:0
constr_1580311056580315000 ==
/\ coord_epoch < 3
/\ start_stop_ctr < 3
----
=============================================================================
\* Modification History
\* Created Wed Jan 29 16:17:36 CET 2020 by GUNMETAL
