#include "../include/global.h"


string g_data_path; 

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
            
            default:
                cout<<"unknown cfg: "<<key<<endl;
                return -1;
        }
    }
    return 0;
}
