Known Limitations
=================
Airtunnel is still a very young project - there are several known
limitations:

-  interaction with a local physical data store has been built out using
   custom operators as a PoC and Airtunnel testing environment – in the
   near future, cloud storage providers should be supported. If you want
   to build your own, see: `Extending Airtunnel <extending.html>`_
-  lineage collection for SQL is implemented fairly simplistic – it
   won't work well for queries with i.e. CTE at the top
-  declaration properties for data assets are at a common, minimum level
   - but might in the future be extended a lot. It's suggested to, in
   the meantime, leverage the non-validated "extra" section-key in the
   YAML, to declare additional properties Airtunnel does not cover.

In general Airtunnel does not attempt to solve any use-case; rather, we
like to stay open for extensions provided on the user side. As such, any
of the shortcomings mentioned aboved can be realized through subclassing
existing Airtunnel classes.