#include "resquecommon.h"

const string cacheFile= "hgskewinput"; // default hdfs cache file name 
const int OBJECT_LIMIT= 5000;

// data type declaration 
map<int, std::vector<Geometry*> > polydata;
map<int, std::vector<string> > rawdata;
ISpatialIndex * spidx = NULL;
IStorageManager * storage = NULL;

struct query_op { 
  int JOIN_PREDICATE;
  int shape_idx_1;
  int shape_idx_2;
  int join_cardinality;
  double expansion_distance;
  vector<int> proj1;
  vector<int> proj2;
} stop; // st opeartor 

void init();
void print_stop();
int joinBucket();
int mJoinQuery(); 
void releaseShapeMem(const int k);
int getJoinPredicate(char * predicate_str);
void setProjectionParam(char * arg);
bool extractParams(int argc, char** argv );
void ReportResult( int i , int j);
string project( vector<string> & fields, int sid);
void freeObjects();
bool buildIndex(map<int,Geometry*> & geom_polygons);

void init(){
  // initlize query operator 
  stop.expansion_distance = 0.0;
  stop.JOIN_PREDICATE = -1;
  stop.shape_idx_1 = -1 ;
  stop.shape_idx_2 = -1 ;
  stop.join_cardinality = 0;
}

void print_stop(){
  // initlize query operator 
  std::cerr << "predicate: " << stop.JOIN_PREDICATE << std::endl;
  std::cerr << "distance: " << stop.expansion_distance << std::endl;
  std::cerr << "shape index 1: " << stop.shape_idx_1 << std::endl;
  std::cerr << "shape index 2: " << stop.shape_idx_2 << std::endl;
  std::cerr << "join cardinality: " << stop.join_cardinality << std::endl;
  std::cerr << "selected fields :" ;
  
  for (int i =0 ; i < stop.proj1.size(); i++)
    std::cerr << " " << stop.proj1[i] ;
  if (stop.proj1.size()==0)
    std::cerr << "all" ;
  std::cerr << endl; 
  
  std::cerr << "selected fields :" ;
  for (int i =0 ; i < stop.proj2.size(); i++)
    std::cerr << " " << stop.proj2[i] ;
  if (stop.proj2.size()==0)
    std::cerr << "all" ;
  std::cerr << endl; 
}

int mJoinQuery()
{
  string input_line;
  vector<string> fields;
  PrecisionModel *pm = new PrecisionModel();
  GeometryFactory *gf = new GeometryFactory(pm,OSM_SRID);
  WKTReader *wkt_reader = new WKTReader(gf);
  Geometry *poly = NULL;
  Geometry *buffer_poly = NULL;

  int object_counter = 0;
  // parse the cache file from Distributed Cache
  std::ifstream skewFile(cacheFile);
  while (std::getline(skewFile, input_line))
  {
    tokenize(input_line, fields, TAB, true);
    if (fields[stop.shape_idx_2].size() < 4) // this number 4 is really arbitrary
      continue ; // empty spatial object 

    try { 
      poly = wkt_reader->read(fields[stop.shape_idx_2]);
    }
    catch (...) {
      std::cerr << "******Geometry Parsing Error******" << std::endl;
      return -1;
    }

    if (ST_DWITHIN == stop.JOIN_PREDICATE && stop.expansion_distance != 0.0 )
    {
      buffer_poly = poly ;
      poly = BufferOp::bufferOp(buffer_poly,stop.expansion_distance,0,BufferParameters::CAP_SQUARE);
      delete buffer_poly ;
    }

    polydata[SID_2].push_back(poly);
    //--- a better engine implements projection 
    rawdata[SID_2].push_back(stop.proj2.size()>0 ? project(fields,SID_2) : input_line); 

    fields.clear();
  }
  skewFile.close();
  // update predicate to st_intersects
  if (ST_DWITHIN == stop.JOIN_PREDICATE )
    stop.JOIN_PREDICATE = ST_INTERSECTS ;

  // parse the main dataset 
  object_counter = 0;
  while(cin && getline(cin, input_line) && !cin.eof()) {
    tokenize(input_line, fields, TAB, true);
    //cerr << "Shape size: " << fields[stop.shape_idx_1].size()<< endl;
    if (fields[stop.shape_idx_1].size() < 4) // this number 4 is really arbitrary
      continue ; // empty spatial object 

    try { 
      // cerr << "Tweet: " << (object_counter+1) << endl; 
      // cerr.flush();
      poly = wkt_reader->read(fields[stop.shape_idx_1]);
    }
    catch (...) {
      std::cerr << "******Geometry Parsing Error******" << std::endl;
      return -1;
    }

    if (object_counter++ % OBJECT_LIMIT == 0 && object_counter >1) {
      int  pairs = joinBucket();
      std::cerr <<rawdata[SID_1].size() << "|x|" << rawdata[SID_2].size() << "|=|" << pairs << "|" <<std::endl;
      releaseShapeMem(1);
    }

    // populate the bucket for join 
    polydata[SID_1].push_back(poly);
    rawdata[SID_1].push_back(stop.proj1.size()>0 ? project(fields,SID_1) : input_line); 

    fields.clear();
  }

  // last batch 
  int  pairs = joinBucket();
  std::cerr <<rawdata[SID_1].size() << "|x|" << rawdata[SID_2].size() << "|=|" << pairs << "|" <<std::endl;
  releaseShapeMem(stop.join_cardinality);

  // clean up newed objects
  delete wkt_reader ;
  delete gf ;
  delete pm ;

  return object_counter;
}

void releaseShapeMem(const int k ){
  if (k <=0)
    return ;
  for (int j =0 ; j <k ;j++ )
  {
    int delete_index = j+1 ;
    int len = polydata[delete_index].size();

    for (int i = 0; i < len ; i++) 
      delete polydata[delete_index][i];

    polydata[delete_index].clear();
    rawdata[delete_index].clear();
  }
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
 
  switch (jp){

    case ST_INTERSECTS:
      flag = env1->intersects(env2) && geom1->intersects(geom2);
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
      buffer_op1 = new BufferOp(geom1);
      // buffer_op2 = new BufferOp(geom2);
      if (NULL == buffer_op1)
        cerr << "NULL: buffer_op1" <<endl;

      geom_buffer1 = buffer_op1->getResultGeometry(stop.expansion_distance);
      // geom_buffer2 = buffer_op2->getResultGeometry(expansion_distance);
      //Envelope * env_temp = geom_buffer1->getEnvelopeInternal();
      if (NULL == geom_buffer1)
        cerr << "NULL: geom_buffer1" <<endl;

      flag = join_with_predicate(geom_buffer1,geom2, env1, env2, ST_INTERSECTS);
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


string project( vector<string> & fields, int sid) {
  std::stringstream ss;
  switch (sid){
    case 1:
      for (int i =0 ; i <stop.proj1.size();i++)
      {
        if ( 0 == i )
          ss << fields[stop.proj1[i]] ;
        else
        {
          if (stop.proj1[i] < fields.size())
            ss << TAB << fields[stop.proj1[i]];
        }
      }
      break;
    case 2:
      for (int i =0 ; i <stop.proj2.size();i++)
      {
        if ( 0 == i )
          ss << fields[stop.proj2[i]] ;
        else{ 
          if (stop.proj2[i] < fields.size())
            ss << TAB << fields[stop.proj2[i]];
        }
      }
      break;
    default:
      break;
  }

  return ss.str();
}

void ReportResult( int i , int j)
{
  switch (stop.join_cardinality){
    case 1:
      cout << rawdata[SID_1][i] << SEP << rawdata[SID_1][j] << endl;
      break;
    case 2:
      cout << rawdata[SID_2][j] << SEP << rawdata[SID_1][i] << endl; 
      break;
    default:
      return ;
  }
}

int joinBucket() 
{
  double low[2], high[2];
  // cerr << "---------------------------------------------------" << endl;
  int pairs = 0;
  bool selfjoin = stop.join_cardinality ==1 ? true : false ;
  int idx1 = SID_1 ; 
  int idx2 = selfjoin ? SID_1 : SID_2 ;

  // for each tile (key) in the input stream 
  try { 

    std::vector<Geometry*>  & poly_set_one = polydata[idx1];
    std::vector<Geometry*>  & poly_set_two = polydata[idx2];

    int len1 = poly_set_one.size();
    int len2 = poly_set_two.size();
    
    map<int,Geometry*> geom_polygons2;
    for (int j = 0; j < len2; j++) {
        geom_polygons2[j] = poly_set_two[j];
    }
    
    // build spatial index for input polygons from idx2
    bool ret = buildIndex(geom_polygons2);
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
              ReportResult(i,hits[j]);
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
  return pairs ;
}


bool buildIndex(map<int,Geometry*> & geom_polygons) {
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


void freeObjects() {
    // garbage collection
    delete spidx;
    delete storage;
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
    {0, 0, 0, 0}
  };


  int c;
  while ((c = getopt_long (argc, argv, "p:i:j:d:f:",long_options, &option_index)) != -1){
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
        break;

      case 'i':
        stop.shape_idx_1 = strtol(optarg, NULL, 10) - 1;
        stop.join_cardinality++;
        //printf ("shape index i: `%s'\n", optarg);
        break;

      case 'j':
        stop.shape_idx_2 = strtol(optarg, NULL, 10) - 1;
        stop.join_cardinality++;
        break;

      case 'd':
        stop.expansion_distance = atof(optarg);
        break;

      case 'f':
        setProjectionParam(optarg);
        //printf ("projection fields:  `%s'\n", optarg);
        break;

      case '?':
        return false;
        /* getopt_long already printed an error message. */
        break;

      default:
        return false;
        //abort ();
    }
  }

  // query operator validation 
  if (stop.JOIN_PREDICATE <= 0 )// is the predicate supported 
  { 
    cerr << "Query predicate is NOT set properly. Please refer to the documentation." << endl ; 
    return false;
  }
  // if the distance is valid 
  if (ST_DWITHIN == stop.JOIN_PREDICATE && stop.expansion_distance == 0.0)
  { 
    cerr << "Distance parameter is NOT set properly. Please refer to the documentation." << endl ;
    return false;
  }
  if (0 == stop.join_cardinality)
  {
    cerr << "Geometry field indexes are NOT set properly. Please refer to the documentation." << endl ;
    return false; 
  }

  print_stop();

  return true;
}

void setProjectionParam(char * arg)
{
  string param(arg);
  vector<string> fields;
  vector<string> selec;
  tokenize(param, fields,":");

  if (fields.size()>0)
  {
    tokenize(fields[0], selec,",");
    for (int i =0 ;i < selec.size(); i++)
      stop.proj1.push_back(atoi(selec[i].c_str())-1);
  }
  selec.clear();

  if (fields.size()>1)
  {
    tokenize(fields[1], selec,",");
    for (int i =0 ;i < selec.size(); i++)
      stop.proj2.push_back(atoi(selec[i].c_str())-1);
  }
}

int getJoinPredicate(char * predicate_str)
{
  if (strcmp(predicate_str, "st_intersects") == 0) {
    // stop.JOIN_PREDICATE = ST_INTERSECTS;
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
    // std::cerr << "unrecognized join predicate " << std::endl;
    return 0;
  }
}

void usage(){
  cerr  << endl << "Usage: skewresque [OPTIONS]" << endl << "OPTIONS:" << endl;
  cerr << TAB << "-p,  --predicate" << TAB <<  "The spatial join predicate for query processing. AccepTABle values are [st_intersects, " 
      << "st_disjoint, st_overlaps, st_within, st_equals, st_dwithin, st_crosses, st_touches, st_contains]." << endl;
  cerr << TAB << "-i, --shpidx1"  << TAB << "The index of the geometry field from the larger dataset. Index value starts from 1." << endl;
  cerr << TAB << "-j, --shpidx2"  << TAB << "The index of the geometry field from the smaller dataset. Index value starts from 1." << endl;
  cerr << TAB << "-d, --distance" << TAB << "Used together with st_dwithin predicate to indicates the join distance." 
      << "This field has no effect o other join predicates." << endl;
  cerr << TAB << "-f, --fields"   << TAB << "Output field election parameter. Fields from different dataset are separated with a colon (:), " 
      <<"and fields from the same dataset are separated with a comma (,). For example: if we want to only output fields 1, 3, and 5 from " 
      << "the first dataset (indicated with param -i), and output fields 1, 2, and 9 from the second dataset (indicated with param -j) "
      << " then we can provide an option such as: --fields 1,3,5:1,2,9 " << endl;
}

// main body of the engine
int main(int argc, char** argv)
{
  // initilize query operator 
  init();

  int c = 0 ;
  if (!extractParams(argc,argv)) {
    // std::cerr <<"ERROR: query parameter extraction error." << std::endl << "Please see documentations, or contact author." << std::endl;
    usage();
    return 1;
  }

  switch (stop.join_cardinality){
    case 1:
    case 2:
      c = mJoinQuery();
      // std::cerr <<"ERROR: input data parsing error." << std::endl << "Please see documentations, or contact author." << std::endl;
      break;

    default:
      std::cerr <<"ERROR: join cardinality does not match engine capacity." << std::endl ;
      return 1;
  }

  if (c >= 0 )
    std::cerr <<"Query Load: [" << c << "]" <<std::endl;
  else 
  {
    std::cerr <<"Error: ill formatted data. Terminating ....... " << std::endl;
    return 1;
  }

  freeObjects();
  cout.flush();
  cerr.flush();

  return 0;
}

