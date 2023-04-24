from django.contrib import messages
from django.shortcuts import render, redirect
from django.contrib.auth.models import User, auth
from django.views.decorators.csrf import csrf_exempt

from elasticsearch import Elasticsearch
es = Elasticsearch("http://localhost:9200")

@csrf_exempt
def home(request):
    return render(request, 'home.html')
    if request.method == 'POST':
        query = request.POST['query']
        return redirect('search')

@csrf_exempt
def logout_user(request):
    auth.logout(request)
    # messages.success(request, "You have been Logged Out")
    return redirect('home')

@csrf_exempt
def login_user(request):
    if request.method == 'POST':
        username = request.POST['username']
        password = request.POST['password']

        user = auth.authenticate(username=username, password=password)

        if user is not None:
            auth.login(request, user)
            # messages.info(request, "You Have been Logged In")
            return redirect('home')
        else:
            messages.info(request, 'Invalid Username or Password')
            return redirect('login_user')
    else:
        return render(request, 'login.html')

@csrf_exempt
def register(request):
    if request.method == 'POST':
        first_name = request.POST['first_name']
        last_name = request.POST['last_name']
        username = request.POST['username']
        email = request.POST['email']
        password = request.POST['password']
        confirm_password = request.POST['confirm_password']

        if password==confirm_password:
            if User.objects.filter(username=username).exists():
                messages.info(request, 'Username is already taken')
                return redirect(register)
            elif User.objects.filter(email=email).exists():
                messages.info(request, 'Email is already taken')
                return redirect(register)
            else:
                user = User.objects.create_user(username=username, password=password, 
                                        email=email, first_name=first_name, last_name=last_name)
                user.save()
                auth.login(request, user)
                # messages.success(request, "You have successfully registered")
                return redirect('home')
        else:
            messages.info(request, 'Both passwords are not matching')
            return redirect('register')
    else:
        return render(request, 'registeration.html')

@csrf_exempt
def search(request):
    if request.method == 'POST':
        query = request.POST['query']
        results = es.search(index='my_index', body={
            'query': {
                'multi_match': {
                    'query': query,
                    'fields': ['title', 'url', 'content']
                }
            },
        })['hits']['hits']
        res = []
        from datetime import datetime

        for line in results:
            res.append({
	            'url': line['_source']['url'],
            	'title': line['_source']['title'],
	            'published_at': datetime.strptime(line['_source']['published_at'], "%Y-%m-%dT%H:%M:%S").strftime("%d/%m/%Y")
            })
        return render(request, 'search.html', {'query': query, 'results': res})
    return render(request, 'login.html')