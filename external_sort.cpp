#include <bits/stdc++.h>
using namespace std;

bool mergeempty(vector<int>* merge,int base)
{
	bool r=true;
	for(int i=0;i<base;i++)
	{
		if(merge[i].size()!=0)
		{
			r=false;
			break;
		}
	}
	return r;
}

int* createpass0(int* A,int p,int k,int b,int N)
{
	int i;
	for(i=0;i<N;)
	{
		if(i+p*k-1 < N)
		{
			sort(A+i,A+i+p*k);
			i=i+p*k;
		}
		else
		{
			sort(A+i,A+N);
			i=N;
		}
	}
		
	return A;
	
}

int* externalsort(int* A,int p,int k,int b,int passid,int run_size,int N,int count)
{
	int base=(p-b)/b;
	if(run_size>=N)
	{
		return A;
	}
	if(passid==0)
	{
		A=createpass0(A,p,k,b,N);
		int i,count=0;
		/*cout<<"passid = "<<passid<<endl;
		cout<<"Batch 1: ";
		for(i=0;i<N;++i)
		{
			cout<<A[i]<<"  ";
			count++;
			if(count%(p*k)==0)
			{
				cout << endl;
				cout<<"Batch "<<count/(p*k)+1<<": ";	
			}
		}*/
		count=(int)ceil(((double)N)/(p*k)); //no. of runs
		run_size=p*k;
		return externalsort(A,p,k,b,1,run_size,N,count);	
	}
	else
	{
		int B[N];
		int index=0;
		int *endptr[count];
		int *startptr[count];
		for(int i=0;i<count;i++)
		{
			startptr[i]=&A[i*run_size];
			if((i+1)*run_size>=N)
				endptr[i]=&A[N-1];			
			else
				endptr[i]=&A[(i+1)*run_size-1];
		}	
		int present_runs=(int)ceil(((double)count)/base); //no. of batches that will be created in the present pass
		vector <int> merge[base];
		vector <int> buffer;
		for(int i=0;i<present_runs;i++)	
		{
			if(i!=present_runs-1)
			{
				int m=i*base*run_size;			//denotes run from which we start
				for(int j=m;j<m+base*run_size;)		//taking out b pages from base runs
				{			
					for(int t=0;t<b*k;t++)
					{	
						if(startptr[j/run_size]<=endptr[j/run_size])		//fill mergebuffer
						{
							if(merge[(j-m)/run_size].size()<k*b)
							{				
								merge[(j-m)/run_size].push_back(*(startptr[j/run_size]));
								startptr[j/run_size]=1+startptr[j/run_size];
								if(t%k == 0)
									cout <<"Read page fault for R"<<(passid - 1)<<"_"<<(j/run_size)<<" Page["<<(t/k)<<"]"<<endl; 								//new added
							}
						}	
					}
					j+=run_size;
				}
				int flag[base] = {0};
				int buf_flag = 0;	
				while(!mergeempty(merge,base))
				{
					if(buffer.size()==b*k)
					{
						vector <int> :: iterator l;
						for(l=buffer.begin();l!=buffer.end();++l)
						{
							B[index]=*l;
							index++;
						}	
						buffer.erase(buffer.begin(),buffer.end());
					}
					int min=100000,minindex;
					for(k=0;k<base;k++)
					{
						if(merge[k].size())	
						if(min>*(merge[k].begin()))
						{
							min=*(merge[k].begin());
							minindex=k;	
						}
					}
					buffer.push_back(min);
					buf_flag++;
									
					merge[minindex].erase(merge[minindex].begin());
					//if((merge[minindex].size()<b*k) && (merge[minindex].size()%k == 0))
					//cout<<"Read page fault for R"<<(passid - 1)<<"_"<<(i*base+minindex)<<" Page["<<<<"]"<<endl;		
					if(startptr[i*base+minindex]<=endptr[i*base+minindex])
					{
						merge[minindex].push_back(*(startptr[i*base+minindex]));
						flag[minindex]++;
						if(flag[minindex] % k == 0)
						cout<<"Read page fault for R"<<(passid - 1)<<"_"<<(i*base+minindex)<<" Page["<<(((flag[minindex]/k)-1)%b)<<"]"<<endl;
						startptr[i*base+minindex]+=1;
					}
					if(buf_flag % k == 0)
						cout<<"Write page fault for output Page ["<<(((buf_flag/k)-1)%b)<<"]"<<endl;
							
				}	
			}
			else
			{
				int m=i*base*run_size;		//denotes run from which we start runs at a time
				int extra=count%base;
				int sz;
				if(extra==0)
				{
					sz=base;
				}
				else
					sz=extra;
				for(int j=m;j<m+sz*run_size;)		//taking out b pages from base runs
				{
					for(int t=0;t<b*k;t++)
					{	
						if(startptr[j/run_size]<=endptr[j/run_size])		//fill mergebuffer
						{
							if(merge[(j-m)/run_size].size()<k*b)
							{				
								merge[(j-m)/run_size].push_back(*(startptr[j/run_size]));
								startptr[j/run_size]=1+startptr[j/run_size];
								if(t%k == 0)
									cout <<"Read page fault for R"<<(passid - 1)<<"_"<<(j/run_size)<<" Page["<<(t/k)<<"]"<<endl; 								//new added
							}
						}	
						else
							break;
					}
					j+=run_size;
				}
				int flag[sz] = {0};	
				int buf_flag = 0;	
				while(!mergeempty(merge,sz))
				{
					if(buffer.size()==b*k)
					{	
						vector <int> :: iterator l;
						for(l=buffer.begin();l!=buffer.end();++l)
						{
							B[index]=*l;
							index++;
						}
						buffer.erase(buffer.begin(),buffer.end());
					}
					int min=100000,minindex;
					for(k=0;k<sz;k++)		//sz is for runs in prev pass
					{
						if(merge[k].size())	
						if(min>*(merge[k].begin()))
						{
							min=*(merge[k].begin());
							minindex=k;	
						}
					}
					buffer.push_back(min);
					buf_flag++;
					if(buf_flag % k == 0)
						cout<<"Write page fault for output Page ["<<(((buf_flag/k)-1)%b)<<"]"<<endl;
					
					merge[minindex].erase(merge[minindex].begin());
					if(startptr[i*base+minindex]<=endptr[i*base+minindex])
					{
						merge[minindex].push_back(*(startptr[i*base+minindex]));
						flag[minindex]++;
						if(flag[minindex] % k == 0)
							cout<<"Read page fault for R"<<(passid - 1)<<"_"<<(i*base+minindex)<<" Page["<<(((flag[minindex]/k)-1)%b)<<"]"<<endl;
						startptr[i*base+minindex]+=1;
					}	
				}	
			}
		}
		if(buffer.size()!=0)
		{
			vector <int> :: iterator l;
			for(l=buffer.begin();l!=buffer.end();++l)
			{
				B[index]=*l;
				index++;
			}
			buffer.erase(buffer.begin(),buffer.end());
								
		}
		run_size=run_size*base;
		count=present_runs;
		//cout<<endl<<endl<<"passid ="<<passid<<endl;
		passid++;
		/*int batch=1;
		cout<< "Batch " << batch << ": ";
		for(int i=0;i<N;i++)
		{
			cout<<B[i]<<" ";
			if((i+1)%run_size==0)
			{
				cout<<endl<<"Batch " << batch << ": ";
				batch++;
			}
		}
		cout<<endl;*/
		for(int i=0;i<N;i++)
		{
			A[i]=B[i];
		}
		return externalsort(A,p,k,b,passid,run_size,N,count);
	}
}

int main (int argc ,char** argv)
{
	
	char input_file[100];
	strcpy(input_file,argv[1]);
	int p=atoi(argv[2]);
	int k=atoi(argv[3]);
	int b=atoi(argv[4]);
	
	int *A,N;			//create vector A by reading file

	ifstream infile;
	string line;
	infile.open(argv[1]);
	getline(infile, line);
	N=atoi(line.c_str());
	A=new int [N];
	for(int i=0;i<N;i++)
	{
		getline (infile,line);
		A[i]=atoi(line.c_str());
	}
	infile.close();

	externalsort(A,p,k,b,0,1,N,0);
	return 0;
}
