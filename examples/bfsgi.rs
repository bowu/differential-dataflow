extern crate rand;
extern crate timely;
extern crate differential_dataflow;


use std::sync::{Arc, Mutex};
use std::io::BufReader;
use std::error::Error;
use std::fs::File;
use std::io::prelude::*;
use std::path::Path;
//use std::cmp::max;

//use rand::{Rng, SeedableRng, StdRng};

use timely::dataflow::*;
use timely::dataflow::operators::probe::Handle;

use differential_dataflow::input::Input;
use differential_dataflow::Collection;
use differential_dataflow::operators::*;
use differential_dataflow::lattice::Lattice;
use differential_dataflow::logging::DifferentialEvent;

type Node = u32;
type Edge = (Node, Node);

fn main() {

//    let mut nodes = 0;//: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let filename = std::env::args().nth(1).unwrap();
    let streamname = std::env::args().nth(2).unwrap();
    //    let edges: u32 = std::env::args().nth(1).unwrap().parse().unwrap();
    let batch: u32 = std::env::args().nth(3).unwrap().parse().unwrap();
    let rounds: u32 = std::env::args().nth(4).unwrap().parse().unwrap();
    let inspect: bool = std::env::args().nth(5).unwrap() == "inspect";
    
    // load fragment of input graph into memory to avoid io while running.
    
//    let pre_load = std::env::args().nth(1).unwrap().parse().unwrap();

    

    // define a new computational scope, in which to run BFS
    timely::execute_from_args(std::env::args(), move |worker| {

        if let Ok(addr) = ::std::env::var("DIFFERENTIAL_LOG_ADDR") {

            eprintln!("enabled DIFFERENTIAL logging to {}", addr);

            if let Ok(stream) = ::std::net::TcpStream::connect(&addr) {
                let writer = ::timely::dataflow::operators::capture::EventWriter::new(stream);
                let mut logger = ::timely::logging::BatchLogger::new(writer);
                worker.log_register().insert::<DifferentialEvent,_>("differential/arrange", move |time, data|
                    logger.publish_batch(time, data)
                );
            }
            else {
                panic!("Could not connect to differential log address: {:?}", addr);
            }
        }

        let timer = ::std::time::Instant::now();

        // define BFS dataflow; return handles to roots and edges inputs
	let mut probe = Handle::new();
        let (mut roots, mut graph) = worker.dataflow(|scope| {

            let (root_input, roots) = scope.new_collection();
            let (edge_input, graph) = scope.new_collection();
	    //let timer = ::std::time::Instant::now();
            let mut result = bfs(&graph, &roots);
	    
            if !inspect {
                result = result.filter(|_| false);
            }
	    // println!("elapsed time: {:?}",timer.elapsed());   
	    /*
            result.map(|(_,l)| l)
                .consolidate()
                .inspect(|x| println!("theirs\t{:?}", x))
                .probe_with(&mut probe);
	    
	     */
	   // result.map(|(_,l)| (0,l)).inspect(|x| println!("prered{:?}",x));
	       
	    result.map(|(_,l)| (0,l)).reduce( |_k,s,t| {

		//println!("{:?}",s);
		let f = s.iter().fold(0isize, |acc,x| {
		    acc+x.1*(*x.0 as isize)
		});
		println!("total has changed to {}",f);
		t.push((0,f));
	    })
		.inspect(|x| println!("")).probe_with(&mut probe);;
//	    result.inspect(|x| println!("\t{:?}",x)).probe_with(&mut probe);
	    
	    (root_input, edge_input)
        });

        //let seed: &[_] = &[1, 2, 3, 4];
        //let mut rng1: StdRng = SeedableRng::from_seed(seed);    // rng for edge additions
        //let mut rng2: StdRng = SeedableRng::from_seed(seed);    // rng for edge deletions

        roots.insert(0);
        roots.close();
	
	// Open the path in read-only mode, returns `io::Result<File>`
        let mut lines = match File::open(&Path::new(&filename)) {
	    Ok(file) => BufReader::new(file).lines(),
	    Err(why) => {
                    panic!("EXCEPTION: couldn't open {}: {}",
			   Path::new(&filename).display(),
			   Error::description(&why))
	    },
        };
	if worker.index() == 0 {
            // load up the graph, using the first `limit` lines in the file.
            for line in lines.by_ref() {
		// each worker is responsible for a fraction of the queries
		//if counter % peers == index {
                let good_line = line.ok().expect("EXCEPTION: read error");
                if !good_line.starts_with('#') && good_line.len() > 0 {
		    let mut elements = good_line[..].split_whitespace();
		    let src: Node = elements.next().unwrap().parse().ok().expect("malformed src");
		    let dst: Node = elements.next().unwrap().parse().ok().expect("malformed dst");
//		    nodes = max(src+1,nodes);
//		    onodes = max(dst+1,nodes);
		    graph.insert((dst, src));
		    graph.insert((src, dst));
                }
	    }
	}
	lines = match File::open(&Path::new(&streamname)) {
	    Ok(file) => BufReader::new(file).lines(),
	    Err(why) => {
                    panic!("EXCEPTION: couldn't open {}: {}",
			   Path::new(&filename).display(),
			   Error::description(&why))
	    },
        };
	
	
        println!("performing BFS");

	/*
        if worker.index() == 0 {
            for _ in 0 .. edges {
                graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
            }
        }*/

        println!("{:?}\tloaded", timer.elapsed());

        graph.advance_to(1);
        graph.flush();
        worker.step_while(|| probe.less_than(graph.time()));

        println!("{:?}\tstable", timer.elapsed());

        for round in 0 .. rounds {
//            for _element in 0 .. batch {
                if worker.index() == 0{
		    for line in lines.by_ref().take(batch as usize){
                    //graph.insert((rng1.gen_range(0, nodes), rng1.gen_range(0, nodes)));
                    //graph.remove((rng2.gen_range(0, nodes), rng2.gen_range(0, nodes)));
			let good_line = line.ok().expect("EXCEPTION: read error");
			if !good_line.starts_with('#') && good_line.len() > 0 {
			    let mut elements = good_line[..].split_whitespace();
			    let ins: u32 = elements.next().unwrap().parse().ok().expect("malformed spec");
			    let src: Node = elements.next().unwrap().parse().ok().expect("malformed src");
			    let dst: Node = elements.next().unwrap().parse().ok().expect("malformed dst");
			    
			    if ins == 3 {
				//println!("inserting {} {}",src,dst);
				graph.insert((src, dst));
				graph.insert((dst, src));
			    }else if ins == 4{
				//println!("removing {} {}",src,dst);
				graph.remove((src,dst));
				graph.remove((dst,src));
			    }else{
				println!("malformed input"); 
			    }
			}
		    }
  //              }
                
            }
	    graph.advance_to(2 + round * batch + batch-1);
            graph.flush();

            let timer2 = ::std::time::Instant::now();
            worker.step_while(|| probe.less_than(&graph.time()));

            if worker.index() == 0 {
                let elapsed = timer2.elapsed();
                println!("{:?}\t{:?}: batch took \t{}ns", timer.elapsed(), round, elapsed.as_secs() * 1000000000 + (elapsed.subsec_nanos() as u64));
            }
        }
        println!("finished; elapsed: {:?}", timer.elapsed());
    }).unwrap();
}

// returns pairs (n, s) indicating node n can be reached from a root in s steps.
fn bfs<G: Scope>(edges: &Collection<G, Edge>, roots: &Collection<G, Node>) -> Collection<G, (Node, u32)>
where G::Timestamp: Lattice+Ord {

    // initialize roots as reaching themselves at distance 0
    let nodes = roots.map(|x| (x, 0));

    // repeatedly update minimal distances each node can be reached from each root
    nodes.iterate(|inner| {

        let edges = edges.enter(&inner.scope());
        let nodes = nodes.enter(&inner.scope());
        inner.join_map(&edges, |_k,l,d| (*d, l+1))
             .concat(&nodes)
             .reduce(|_, s, t| t.push((*s[0].0, 1)))
     })
}
