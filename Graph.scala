import org.apache.spark.SparkContext
import org.apache.spark.SparkConf

/*
	Project: ApacheSpark-GraphProcessing-ConnectedComponents
	Author: Vaibhav Murkute
*/

@SerialVersionUID(123L)
case class Vertex ( tag: Long, group: Long, vid: Long, adjacent:List[String] )
      extends Serializable {}


object Graph {
  def main(args: Array[ String ]) {
    val conf = new SparkConf().setAppName("GraphProcessing");
    val sc = new SparkContext(conf);
    val vertices = sc.textFile(args(0)).map( line => { var input = line.split(",");
    	var vid = input(0).toLong;
    	var tag = 0.toLong;
    	var adj = input.toList.tail;
    	Vertex(tag, vid, vid, adj);
    })
   
    var v_mapper = vertices.map( vertices => (vertices.vid,vertices))
    	.map { case (k, vertices) =>
    		var v_tag = vertices.tag;
    		var v_vid = vertices.vid;
    		var v_grp = vertices.group;
    		var v_adj = vertices.adjacent;
    		
		(v_vid, Vertex(v_tag, v_grp, v_vid, v_adj))
	 }
	
	var idx = 0;
	// Hacky-variable-Initialization! :D
	var mapper1 = v_mapper.values.map( v_mapper => (v_mapper.vid,v_mapper));
	var mapper2 = v_mapper.values.map( v_mapper => (v_mapper.vid,v_mapper));
	var reducer = v_mapper.values.map( v_mapper => (v_mapper.vid,v_mapper));

  	for(idx <- 1 to 5) { 
		mapper1 = v_mapper.values.map( v_mapper => (v_mapper.vid,v_mapper));
		mapper2 = v_mapper.flatMap(v_mapper => for (j <- v_mapper._2.adjacent)
			yield {
				(j.toLong,new Vertex(1.toLong,v_mapper._2.group,0.toLong,List(0.toString)))
			});

		var v_union = mapper1.union(mapper2).groupByKey();

		reducer = v_union.map(v_union =>
			{
				var min = Long.MaxValue;
				var tmp_list : List[String] = List();
				for (i <- v_union._2) {
					if (i.tag == 0){
						tmp_list = i.adjacent;
					}
					if (i.group < min){
						min = i.group;
					}

				}
				
				(min,Vertex(0.toLong,min,v_union._1,tmp_list))
			})

		v_mapper=reducer;
   	}
	
	var result = reducer.map(reducer => (reducer._1,1.toLong)).reduceByKey(_+_);
   	result.collect.foreach(println);
    sc.stop();
}}

