#include "global.h"


string g_data_path; 
bool g_bulk_load = false;
float g_read_ratio = 0.5;

int Parse(string cfgfile){
    ifstream filestream(cfgfile, ios_base::in);
    if (filestream.fail()) {
        cerr << "open cfgfile:" << cfgfile << " fails!\n";
        return -1;
    }
    string line;

    while(getline(filestream, line)) {
        if (line.size()<=1 || line[0]== '#')
            continue;

        stringstream ss(line);
        string key, value;
        getline(ss, key, ' ');
        getline(ss, value, ' ');

        switch(hash_(key.c_str())){
            case hash_("g_data_path"):
                g_data_path = value;
                break;
            case hash_("g_bulk_load"):
                g_bulk_load = stoi(value);
                break;
            case hash_("g_read_ratio"):
                g_read_ratio = stof(value);
                break;
            
            
            default:
                cout << "unknown cfg: " << key << endl;
                return -1;
        }
    }
    return 0;
}
