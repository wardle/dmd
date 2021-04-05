# dmd

Implementation of the UK NHS dictionary of medicines and devices (dm+d).

*This is a work-in-progress*.

This is a lightweight library and microservice for UK dm+d data. It is a thin
wrapper around the dm+d data; providing a fast key value backing store and search functionality.

It is designed to be composable with other services, including [hermes](https://github.com/wardle/hermes). 
Fortunately, dm+d codes are actually SNOMED identifiers, which means that as long as you run a 
terminology service that includes the UK drug extension, you will be able to seamlessly
process data across both `hermes` and `dmd`.

SNOMED releases occur at three-monthly intervals. 
The NHS Business Services Authority (BSA) release dm+d data weekly.
That means there may be products within dm+d that are not yet within the UK
SNOMED CT drug extension. That was one of the reasons I decided to create `dmd`
as a separate service.

### What is the use of `dmd`?

dmd provides a simple library, and optionally a microservice, to help
you make use of the UK dm+d.

A library can be embedded into your application; this is easy using Clojure or 
Java. You make calls using the dmd API just as you'd use any regular library.

A microservice runs independently and you make use of the data and software
by making an API call over the network.

Like all `PatientCare` components, you can use `dmd` in either way. 
Usually, when you're starting out, it's best to use as a library but larger 
projects and larger installations will want to run their software components
independently, optimising for usage patterns, resilience, reliability and 
rate of change.

### Why do I need separate tooling for dm+d?

Previously, I implemented dm+d within an EPR and later within a SNOMED terminology server. 
However, the UK drug extension lacks some of the data that is included in the NHS BSA dm+d
distribution, such as the concrete data that are needed in order to safely 
map from dose-based prescribing to product-based prescribing. 
It is difficult to build software that processes prescribing data without those
concrete values documenting how much of each ingredient is in each product.

`dmd` provides data and computing services for the full UK `dm+d` dataset.
It's there if you need to supplement the UK SNOMED drug extension with 
additional data. That will almost certainly be the case if you're writing
an e-prescribing system.

If the SNOMED-based UK drug extension provides all of the information you need
in your user-facing applications, you won't need to also include `dm+d`.

### Why are you building so many small repositories? 

Small modules of functionality are easier to develop, easier to understand,
easier to test and easier to maintain. I design modules to be composable so that I can
stitch different components together in order to solve problems. 

In larger systems, it is easy to see code rotting. Dependencies become outdated and
the software becomes difficult to change easily because of software that depend
on it. Small, well-defined modules are much easier to build and are less likely
to need ongoing changes over time; my goal is to need to update modules only
in response to changes in *domain* not software itself. Accretion of functionality.

It is very difficult to 'prove' software is working as designed when there are
lots of moving parts. 