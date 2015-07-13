#include "resquecommon.h"

/* 
 * RESQUE processing engine v2.0
 *   1) parseParameters
 *   2) readCacheFile - metadata such as partition schemata
 *   3) for every input line in the current tile
 *         an input line represents an object
 *         save geometry and original data in memory
 *         execute join operation when finish reading a tile
 *   4) Join operation between 2 sets or a single set
 *         build Rtree index on the second data set
 *         for every object in the first data set
 *            using Rtree index of the second data set
 *              check for MBR/envelope intersection
 *              output the pair result or save pair statistics
 *   5) Output final statistics (opt)
 *
 *   Requirement (input files): see the Wiki
 * */



/* Spatial index support */

double area1 = -1;
double area2 = -1;
double union_area = -1;
double intersect_area = -1;

/* Query operator */
struct query_op { 
	int JOIN_PREDICATE; /* Join predicate - see resquecommon.h for the full list*/
	int shape_idx_1; /* geometry field number of the 1st set */
	int shape_idx_2; /* geometry field number of the 2nd set */
	int join_cardinality; /* Join cardinality */
	double expansion_distance; /* Distance used in dwithin query */
	vector<int> proj1; /* Output fields for 1st set  */
	vector<int> proj2; /* Output fields for 2nd set */

	/* Output */
	vector<int> append_stats; // appending statistics to the output
	bool append_tile_id; // appending tile_id to the output
} stop; 

/* Function prototypes */
void init();
void print_stop();
int joinBucket(string tile_id, map<int, std::vector<Geometry*> >  polydata,
                map<int,std::vector<string> > rawdata);
int executeQuery(); 
int getJoinPredicate(char * predicate_str);
void releaseShapeMem(const int join_cardinality, map<int, std::vector<Geometry*> >  polydata, 
		map<int,std::vector<string> > rawdata);
void setProjectionParam(char * arg);
void setAppendStats(char *arg);
bool extractParams(int argc, char** argv );
void ReportResult( int i , int j, string tile_id, map<int, std::vector<string> > rawdata);
string project( vector<string> & fields, int sid);
void freeObjects();

void init(){
	// initlize query operator 
	stop.expansion_distance = 0.0;
	stop.JOIN_PREDICATE = 0;
	stop.shape_idx_1 = 0;
	stop.shape_idx_2 = 0 ;
	stop.join_cardinality = 0;
	stop.append_tile_id = false;
}

void print_stop(){
	// initlize query operator 
	std::cerr << "predicate: " << stop.JOIN_PREDICATE << std::endl;
	std::cerr << "distance: " << stop.expansion_distance << std::endl;
	std::cerr << "shape index 1: " << stop.shape_idx_1 << std::endl;
	std::cerr << "shape index 2: " << stop.shape_idx_2 << std::endl;
	std::cerr << "join cardinality: " << stop.join_cardinality << std::endl;
}

int executeQuery()
{
	// Storage for input data
	map<int, std::vector<Geometry*> > polydata;
	map<int, std::vector<string> > rawdata;

	// Processing variables
	string input_line; // Temporary line
	vector<string> fields; // Temporary fields
	int sid = 0; // Join index ID for the current object
	int index = -1;  // Geometry field position for the current object
	string tile_id = ""; // The current tile_id
	string previd = ""; // the tile_id of the previously read object
	int tile_counter = 0; // number of processed tiles

	/* GEOS variables for spatial computation */
	PrecisionModel *pm = new PrecisionModel();
	GeometryFactory *gf = new GeometryFactory(pm, OSM_SRID); // default is OSM for spatial application
	WKTReader *wkt_reader = new WKTReader(gf);
	Geometry *poly = NULL;


#ifdef DEBUG
	std::cerr << "Bucketinfo:[ID] |A|x|B|=|R|" <<std::endl;
#endif  

	while(cin && getline(cin, input_line) && !cin.eof()) {
		tokenize(input_line, fields, TAB, true);
		sid = atoi(fields[1].c_str());
		tile_id = fields[0];

		switch(sid){
			case SID_1:
				index = stop.shape_idx_1 ; 
				break;
			case SID_2:
				index = stop.shape_idx_2 ; 
				break;
			default:
				std::cerr << "wrong sid : " << sid << endl;
				return false;
		}

		/* Handling of objects with missing geometry */
		if (fields[index].size() <= 0) 
			continue ; //skip empty spatial object 

#ifdef DEBUG
		cerr << "geometry: " << fields[stop.shape_idx_1]<< std::endl;
#endif  

		/* Parsing polygon input */
		try { 
			poly = wkt_reader->read(fields[index]);
		}
		catch (...) {
			std::cerr << "******Geometry Parsing Error******" << std::endl;
			return -1;
		}

		/* Process the current tile (bucket) when finishing reading all objects belonging
		   to the current tile */
		if (previd.compare(tile_id) != 0 && previd.size() > 0 ) {
			int  pairs = joinBucket(previd, polydata, rawdata); // number of satisfied predicates
#ifdef DEBUG
			std::cerr <<"T[" << previd << "] |" << polydata[SID_1].size() << "|x|" 
				<< polydata[SID_2].size() << "|=|" << pairs << "|" <<std::endl;
#endif  
			tile_counter++; 
			releaseShapeMem(stop.join_cardinality, polydata, rawdata);
		}

		// populate the bucket for join 
		polydata[sid].push_back(poly);
		switch(sid){
			case SID_1:
				rawdata[sid].push_back(project(fields,SID_1));
				break;
			case SID_2:
				rawdata[sid].push_back(project(fields,SID_2));
				break;
			default:
#ifdef DEBUG
				std::cerr << "wrong sid : " << sid << endl;
#endif  
				return false;
		}

		/* Update the field */
		previd = tile_id; 
		fields.clear();
	}
	// Process the last tile (what remains in memory)
	int  pairs = joinBucket(tile_id, polydata, rawdata);
#ifdef DEBUG
	cerr <<"T[" << previd << "] |" << polydata[SID_1].size() << "|x|" 
		<< polydata[SID_2].size() << "|=|" << pairs << "|" << endl;
#endif  
	tile_counter++;

	releaseShapeMem(stop.join_cardinality, polydata, rawdata);
	
	// clean up newed objects
	delete wkt_reader;
	delete gf;
	delete pm;
//	delete spidx;
//	delete storage;

	return tile_counter;
}

void releaseShapeMem(const int join_cardinality, map<int, std::vector<Geometry*> >  polydata, 
			map<int, std::vector<string> > rawdata) {
	if (join_cardinality <= 0) {
		return ;
	}
  	for (int j =0 ; j < join_cardinality; j++ ) {
    		int delete_index = j+1 ;
    		int len = polydata[delete_index].size();
    		for (int i = 0; i < len ; i++) {
      			delete polydata[delete_index][i];
		}
    		polydata[delete_index].clear();
    		rawdata[delete_index].clear();
  	}
}

bool buildIndex(map<int,Geometry*> & geom_polygons, ISpatialIndex* & spidx, IStorageManager* & storage) {
	// build spatial index on tile boundaries 
	id_type  indexIdentifier;
	GEOSDataStream stream(&geom_polygons);
	storage = StorageManager::createNewMemoryStorageManager();
	spidx   = RTree::createAndBulkLoadNewRTree(RTree::BLM_STR, stream, *storage, 
			FillFactor,
			IndexCapacity,
			LeafCapacity,
			2, 
			RTree::RV_RSTAR, indexIdentifier);

	// Error checking 
	return spidx->isIndexValid();
}


bool join_with_predicate(const Geometry * geom1 , const Geometry * geom2, 
		const Envelope * env1, const Envelope * env2,
		const int jp){
	bool flag = false ; 
	//  const Envelope * env1 = geom1->getEnvelopeInternal();
	//  const Envelope * env2 = geom2->getEnvelopeInternal();
	BufferOp * buffer_op1 = NULL ;
	BufferOp * buffer_op2 = NULL ;
	Geometry* geom_buffer1 = NULL;
	Geometry* geom_buffer2 = NULL;
	Geometry* geomUni = NULL;
	Geometry* geomIntersect = NULL; 

	switch (jp){

		case ST_INTERSECTS:
			#ifdef DEBUG
			cerr << "1: " << env1->toString() << " and " << geom1->toString() << endl;
			cerr << "2: " << env2->toString() << " and " << geom2->toString() << endl;
			#endif

			flag = env1->intersects(env2) && geom1->intersects(geom2);
			if (flag) {
				area1 = geom1->getArea();
				area2 = geom2->getArea();
				geomUni = geom1->Union(geom2);
				union_area = geomUni->getArea();
				geomIntersect = geom1->intersection(geom2);
				intersect_area = geomIntersect->getArea();
				delete geomUni;
				delete geomIntersect;
			}
			break;

		case ST_TOUCHES:
			flag = geom1->touches(geom2);
			break;

		case ST_CROSSES:
			flag = geom1->crosses(geom2);
			break;

		case ST_CONTAINS:
			flag = env1->contains(env2) && geom1->contains(geom2);
			break;

		case ST_ADJACENT:
			flag = ! geom1->disjoint(geom2);
			break;

		case ST_DISJOINT:
			flag = geom1->disjoint(geom2);
			break;

		case ST_EQUALS:
			flag = env1->equals(env2) && geom1->equals(geom2);
			break;

		case ST_DWITHIN:
			/* Special (exact) spatial handling for point-point distance case */
			if (geom1->getGeometryTypeId() == geos::geom::GEOS_POINT &&
					geom2->getGeometryTypeId() == geos::geom::GEOS_POINT) {
				const geos::geom::Point* p1 = dynamic_cast<const geos::geom::Point*>(geom1);
				const geos::geom::Point* p2 = dynamic_cast<const geos::geom::Point*>(geom2);
				flag = pow(p1->getX() - p2->getX(), 2) + pow(p1->getY() - p2->getY(), 2) <= pow(stop.expansion_distance, 2);
				break;
			}

			buffer_op1 = new BufferOp(geom1);
			// buffer_op2 = new BufferOp(geom2);
			if (NULL == buffer_op1)
				cerr << "NULL: buffer_op1" <<endl;

			geom_buffer1 = buffer_op1->getResultGeometry(stop.expansion_distance);
			env1 = geom_buffer1->getEnvelopeInternal();
			// geom_buffer2 = buffer_op2->getResultGeometry(expansion_distance);
			//Envelope * env_temp = geom_buffer1->getEnvelopeInternal();
			if (NULL == geom_buffer1)
				cerr << "NULL: geom_buffer1" <<endl;

			flag = join_with_predicate(geom_buffer1,geom2, env1, env2, ST_INTERSECTS);
			delete geom_buffer1;

			break;

		case ST_WITHIN:
			flag = geom1->within(geom2);
			break; 

		case ST_OVERLAPS:
			flag = geom1->overlaps(geom2);
			break;

		default:
			std::cerr << "ERROR: unknown spatial predicate " << endl;
			break;
	}
	return flag; 
}

/* Filter selected fields for output
 * If there is no field selected, output all fields (except tileid and joinid) */
string project( vector<string> & fields, int sid) {
	std::stringstream ss;
	switch (sid){
		case 1:
			if (stop.proj1.size() == 0) {
				/* Do not output tileid and joinid */
				ss << fields[2];
				for (int i = 3 ; i < fields.size(); i++)
				{
					ss << TAB << fields[i];
				}
			} else {
				for (int i = 0 ; i <stop.proj1.size();i++)
				{
					if ( 0 == i )
						ss << fields[stop.proj1[i]] ;
					else
					{
						if (stop.proj1[i] < fields.size())
							ss << TAB << fields[stop.proj1[i]];
					}
				}
			}
			break;
		case 2:
			if (stop.proj2.size() == 0) {
				/* Do not output tileid and joinid */
				ss << fields[2];
				for (int i = 3 ; i < fields.size(); i++)
				{
					ss << TAB << fields[i];
				}
			} else {
				for (int i = 0 ; i <stop.proj2.size();i++)
				{
					if ( 0 == i )
						ss << fields[stop.proj2[i]] ;
					else
					{
						if (stop.proj2[i] < fields.size())
							ss << TAB << fields[stop.proj2[i]];
					}
				}
			}
			break;
		default:
			break;
	}

	return ss.str();
}

/* Set output fields
 * Fields are "technically" off by 3 (2 from extra field 
 * and 1 because of counting from 1 ) 
 */
void setProjectionParam(char * arg)
{
	string param(arg);
	vector<string> fields;
	vector<string> selec;
	tokenize(param, fields,":");

	if (fields.size() > 0) {
		tokenize(fields[0], selec,",");
		for (int i = 0 ;i < selec.size(); i++) {
			stop.proj1.push_back(atoi(selec[i].c_str()) + 2);
		}
	}
	selec.clear();

	if (fields.size()>1)
        {
                 tokenize(fields[1], selec,",");
                 for (int i =0 ;i < selec.size(); i++) {
                         stop.proj2.push_back(atoi(selec[i].c_str()) + 2);
		}
        }
}

/* Set statistics to be output */
void setAppendStats(char * arg)
{
	string param(arg);
	vector<string> fields;
	tokenize(param, fields,",");

	for (int i = 0; i < fields.size(); i++) {
		string stat_param = fields[i];
		if (stat_param.compare(PARAM_STATS_AREA_1) == 0 ) {
			stop.append_stats.push_back(STATS_UNION_AREA);
		}
		else if (stat_param.compare(PARAM_STATS_AREA_2) == 0) {
			stop.append_stats.push_back(STATS_AREA_2);
		}
		else if (stat_param.compare(PARAM_STATS_UNION_AREA) == 0) {
			stop.append_stats.push_back(STATS_UNION_AREA);
		}
		else if (stat_param.compare(PARAM_STATS_INTERSECT_AREA) == 0) {
			stop.append_stats.push_back(STATS_INTERSECT_AREA);
		}
		else if (stat_param.compare(PARAM_STATS_JACCARD_COEF ) == 0) {
			stop.append_stats.push_back(STATS_JACCARD_COEF);
		}
		else if (stat_param.compare(PARAM_STATS_DICE_COEF) == 0) {	
			stop.append_stats.push_back(STATS_DICE_COEF);
		} else {
#ifdef DEBUG
			cerr << "Unrecognizeable option for output statistics" << endl;
#endif
		}
	}
	std::sort(stop.append_stats.begin(), stop.append_stats.end());
}


int getJoinPredicate(char * predicate_str)
{
	if (strcmp(predicate_str, "st_intersects") == 0) {
		return ST_INTERSECTS ; 
	} 
	else if (strcmp(predicate_str, "st_touches") == 0) {
		return ST_TOUCHES;
	} 
	else if (strcmp(predicate_str, "st_crosses") == 0) {
		return ST_CROSSES;
	} 
	else if (strcmp(predicate_str, "st_contains") == 0) {
		return ST_CONTAINS;
	} 
	else if (strcmp(predicate_str, "st_adjacent") == 0) {
		return ST_ADJACENT;
	} 
	else if (strcmp(predicate_str, "st_disjoint") == 0) {
		return ST_DISJOINT;
	}
	else if (strcmp(predicate_str, "st_equals") == 0) {
		return ST_EQUALS;
	}
	else if (strcmp(predicate_str, "st_dwithin") == 0) {
		return ST_DWITHIN;
	}
	else if (strcmp(predicate_str, "st_within") == 0) {
		return ST_WITHIN;
	}
	else if (strcmp(predicate_str, "st_overlaps") == 0) {
		return ST_OVERLAPS;
	}
	else {
#ifdef DEBUG
		cerr << "unrecognized join predicate " << std::endl;
#endif
		return -1;
	}
}

/* Report result separated by separator */
void ReportResult(int i, int j, string tile_id, map<int, std::vector<string> > rawdata)
{
       


	switch (stop.join_cardinality){
		case 1:
			cout << rawdata[SID_1][i] << SEP << rawdata[SID_1][j] << endl;
			break;
		case 2:
			cout << rawdata[SID_1][i] << SEP << rawdata[SID_2][j]; 
			if (!stop.append_stats.empty()) {
				cout << SEP << area1 << TAB << area2 << TAB << union_area 
					<< TAB << intersect_area << TAB << intersect_area / union_area;
			}
			if (stop.append_tile_id) {
				cout << TAB << tile_id << endl; 
			}
			cout << endl;
			break;
		default:
			return ;
	}
}

void ReportResult(int i, int j, string tile_id, map<int, std::vector<string> > rawdata)
{

}

int joinBucket(string tile_id, map<int, std::vector<Geometry*> >  polydata,
			                map<int,std::vector<string> > rawdata) 
{
	int pairs = 0;
	bool selfjoin = stop.join_cardinality == 1  ? true : false;
	int idx1 = SID_1 ; 
	int idx2 = selfjoin ? SID_1 : SID_2 ;
	double low[2], high[2];

	ISpatialIndex *spidx = NULL;
	IStorageManager *storage = NULL;
	
	// for each tile (key) in the input stream 
	try { 

		std::vector<Geometry*>  & poly_set_one = polydata[idx1];
		std::vector<Geometry*>  & poly_set_two = polydata[idx2];

		
		int len1 = poly_set_one.size();
		int len2 = poly_set_two.size();

		if (len1 <= 0 || len2 <= 0) {
			return 0;
		}

		map<int,Geometry*> geom_polygons2;
		for (int j = 0; j < len2; j++) {
			geom_polygons2[j] = poly_set_two[j];
		}

		// build spatial index for input polygons from idx2
		bool ret = buildIndex(geom_polygons2, spidx, storage);
		if (ret == false) {
			return -1;
		}
		// cerr << "len1 = " << len1 << endl;
		// cerr << "len2 = " << len2 << endl;

		for (int i = 0; i < len1; i++) {
			const Geometry* geom1 = poly_set_one[i];
			const Envelope * env1 = geom1->getEnvelopeInternal();
			low[0] = env1->getMinX();
			low[1] = env1->getMinY();
			high[0] = env1->getMaxX();
			high[1] = env1->getMaxY();
			/* Handle the buffer expansion for R-tree */
			if (stop.JOIN_PREDICATE == ST_DWITHIN) {
				low[0] -= stop.expansion_distance;
				low[1] -= stop.expansion_distance;
				high[0] += stop.expansion_distance;
				high[1] += stop.expansion_distance;
			}
			Region r(low, high, 2);
			hits.clear();
			MyVisitor vis;
			spidx->intersectsWithQuery(r, vis);
			//cerr << "j = " << j << " hits: " << hits.size() << endl;
			for (uint32_t j = 0 ; j < hits.size(); j++ ) 
			{
				if (hits[j] == i && selfjoin) {
					continue;
				}
				const Geometry* geom2 = poly_set_two[hits[j]];
				const Envelope * env2 = geom2->getEnvelopeInternal();
				if (join_with_predicate(geom1, geom2, env1, env2,
							stop.JOIN_PREDICATE))  {
					ReportResult(i, hits[j], tile_id, rawdata);
					pairs++;
				}
			}
		}
	} // end of try
	//catch (Tools::Exception& e) {
	catch (...) {
		std::cerr << "******ERROR******" << std::endl;
		//std::string s = e.what();
		//std::cerr << s << std::endl;
		return -1;
	} // end of catch
	
	delete spidx;
	delete storage;

	return pairs ;
}

bool extractParams(int argc, char** argv ){ 
	/* getopt_long stores the option index here. */
	int option_index = 0;
	/* getopt_long uses opterr to report error*/
	opterr = 0 ;
	struct option long_options[] =
	{
		{"distance",   required_argument, 0, 'd'},
		{"shpidx1",  required_argument, 0, 'i'},
		{"shpidx2",  required_argument, 0, 'j'},
		{"predicate",  required_argument, 0, 'p'},
		{"fields",     required_argument, 0, 'f'},
		{"stats",     required_argument, 0, 's'},
		{"tileid",     required_argument, 0, 't'},
		{0, 0, 0, 0}
	};

	int c;
	while ((c = getopt_long (argc, argv, "p:i:j:d:f:s:t:",long_options, &option_index)) != -1){
		switch (c)
		{
			case 0:
				/* If this option set a flag, do nothing else now. */
				if (long_options[option_index].flag != 0)
					break;
				cout << "option " << long_options[option_index].name ;
				if (optarg)
					cout << "a with arg " << optarg ;
				cout << endl;
				break;

			case 'p':
				stop.JOIN_PREDICATE = getJoinPredicate(optarg);
				#ifdef DEBUG
                                        printf("predicate: %d\n", optarg);
                                #endif
				break;

			case 'i':
				// Adjusting the actual geometry field (shift) to account
				//   for tile_id and join_index
				stop.shape_idx_1 = strtol(optarg, NULL, 10) + 1;
                                stop.join_cardinality++;
                                #ifdef DEBUG
                                        printf("geometry index i (set 1): `%d'\n", optarg);
                                #endif
                                break;

			case 'j':
				stop.shape_idx_2 = strtol(optarg, NULL, 10) + 1;
                                stop.join_cardinality++;
                                #ifdef DEBUG
                                        printf("geometry index j (set 2): `%d'\n", optarg);
                                #endif
                                break;

			case 'd':
				stop.expansion_distance = atof(optarg);
				#ifdef DEBUG
                                        printf("geometry index j (set 2): `%f'\n", optarg);
                                #endif
				break;

			case 'f':
                                setProjectionParam(optarg);
                                #ifdef DEBUG
                                        printf ("projection fields:  `%s'\n", optarg);
                                #endif
                                break;

                        case 's':
                                setAppendStats(optarg);
                                #ifdef DEBUG
                                        printf ("appending statistics at the end:  `%s'\n", optarg);
                                #endif
                                break;

                        case 't':
                                stop.append_tile_id = (strcmp(optarg, "true") == 0);
                                #ifdef DEBUG
                                        printf ("appending tile ID at the end:  `%s'\n", optarg);
                                #endif
                                break;


			case '?':
				return false;
				/* getopt_long already printed an error message. */
				break;

			default:
				return false;
		}
	}

	// query operator validation 
	if (stop.JOIN_PREDICATE <= 0 ) {
		#ifdef DEBUG 
		cerr << "Query predicate is NOT set properly. Please refer to the documentation." << endl ; 
		#endif
		return false;
	}
	// check if distance is set for dwithin predicate
	if (ST_DWITHIN == stop.JOIN_PREDICATE && stop.expansion_distance == 0.0) { 
		#ifdef DEBUG 
		cerr << "Distance parameter is NOT set properly. Please refer to the documentation." << endl ;
		#endif
		return false;
	}
	if (0 == stop.join_cardinality) {
		#ifdef DEBUG 
		cerr << "Geometry field indexes are NOT set properly. Please refer to the documentation." << endl ;
		#endif
		return false; 
	}
	
	#ifdef DEBUG 
	print_stop();
	#endif
	
	return true;
}

/* Clean up variables*/
void freeObjects() {
	// garbage collection
}

/* Display help message to users */
void usage(){
	cerr  << endl << "Usage: resque [OPTIONS]" << endl << "OPTIONS:" << endl;
	cerr << TAB << "-p,  --predicate" << TAB <<  "The spatial join predicate for query processing. \
			Acceptable values are [st_intersects, st_disjoint, st_overlaps, st_within, st_equals,\
			 st_dwithin, st_crosses, st_touches, st_contains]." << endl;
	cerr << TAB << "-i, --shpidx1"  << TAB << "The index of the geometry field from the \
			first dataset. Index value starts from 1." << endl;
	cerr << TAB << "-j, --shpidx2"  << TAB << "The index of the geometry field from the second dataset.\
			 Index value starts from 1." << endl;
	cerr << TAB << "-d, --distance" << TAB << "Used together with st_dwithin predicate to indicates \
			the join distance. This field has no effect o other join predicates." << endl;
	cerr << TAB << "-s, --stats"  << TAB << "(Optional) Include statistics in the result. Arguments\
			are separated by comma. a1=area1, a2=area2, uni=union area, int=intersection area,\
			jac=jaccard coef., dice=dice_coef.\
			e.g. a1,a2,jac,dic " << endl;
	cerr << TAB << "-t, --tileID"  << TAB << "[true | false] Include the statistics \
			in the join output for intersection: area of object 1, area of object 2, \
			union area, intersect area, Jaccard coefficient (tab-separated)." << endl;
	cerr << TAB << "-f, --fields"   << TAB << "Output field election parameter. Fields from \
			different dataset are separated with a colon and fields from the same dataset\
			are separated with a comma (,). For example: if we want to only\
			output fields 1, 3, and 5 from the first dataset (indicated with param -i),\
			and output fields 1, 2, and 9 from the second dataset (indicated with param -j) \
			then we can provide an argument as: --fields 1,3,5:1,2,9 " << endl;
}

/* main body of the engine */
int main(int argc, char** argv)
{
	init();
	int c = 0; // Number of results satisfying the predicate

	if (!extractParams(argc, argv)) {
		#ifdef DEBUG 
		cerr <<"ERROR: query parameter extraction error." << endl 
		     << "Please see documentations, or contact author." << endl;
		#endif
		usage();
		return 1;
	}

	switch (stop.join_cardinality) {
		case 1:
		case 2:
			c = executeQuery();
			break;

		default:
			#ifdef DEBUG 
			std::cerr <<"ERROR: join cardinality does not match engine capacity." << std::endl ;
			#endif
			return 1;
	}
	if (c >= 0 ) {
		#ifdef DEBUG 
		std::cerr <<"Query Load: [" << c << "]" <<std::endl;
		#endif
	} else {
		#ifdef DEBUG 
		std::cerr <<"Error: ill formatted data. Terminating ....... " << std::endl;
		#endif
		return 1;
	}
	freeObjects();

	cout.flush();
	cerr.flush();
	return 0;
}

