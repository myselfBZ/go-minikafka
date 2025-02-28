package api

type FetchRequest struct{
    headers *RequestHeader    
}


func (fr *FetchRequest) Deserialize() {
    
}


func (fr *FetchRequest) Headers() *RequestHeader {
    return fr.headers
}
