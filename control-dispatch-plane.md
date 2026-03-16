The new architecture treats the Lab as the live control system for a broader execution ecosystem.

At the top level, the **Lab** is the place where uncertainty is handled: projects are tracked, work is defined, specs are authored, dispatch decisions are made, and returned results are interpreted back into shared knowledge and next actions. That matches the Lab/Factory boundary already present in your protocol, where the Lab holds Work Items, Specs, and Findings rather than being the place where execution itself happens. 

Within that control system, the core operational structures are the **Lab Projects** database, the **Work Items** database, the **Command Deck**, and the agent instruction layer that governs how Lab agents behave. Those are not side documents; they are part of the architecture’s active surface. The Command Deck exposes operational queues and risk views, Work Items hold executable units of work, Lab Projects provide the project-level frame, and Project Lab governs portfolio-level structure without itself becoming an executor.    

Inside that system, the Lab’s agents have differentiated control-plane roles. **Project Lab** manages project structure, momentum, and context rather than writing code. Other Lab-facing agents handle dispatch, intake, return processing, and synthesis. The Return Protocol sits downstream of execution and is responsible for folding results back into the Lab’s state model, while the Librarian and related flows synthesize what was learned.    

Execution happens in **multiple execution planes** outside that control layer. Those planes include traditional Claude-Code-style agents, GitHub/Factory-style execution, and OpenClaw. OpenClaw is one execution plane among several: it accepts Lab-defined work, executes it through its lane model, and returns structured results through the same broader Lab return discipline. Its contract matters, but only as one execution-plane contract inside the larger Lab architecture.   

So the architecture, in one line, is:

**The Lab is the control plane and knowledge plane; multiple execution planes carry out bounded work; normalized return flows bring outcomes back into Work Items, Projects, and synthesized knowledge.**   

A clean way to summarize the moving parts is:

* **Control plane:** Lab Projects, Work Items, Command Deck, Lab Control, agent instructions, dispatch logic.   
* **Execution planes:** Claude-Code-style agents, OpenClaw lanes, GitHub/Factory execution surfaces.  
* **Return and synthesis plane:** Return Protocol, Intake, Librarian, audit/synthesis flows.  

That makes OpenClaw important, but not central in a way that distorts the workspace. It is one of the Lab’s execution surfaces inside a larger architecture whose real center is the Lab’s orchestration and knowledge loop.
