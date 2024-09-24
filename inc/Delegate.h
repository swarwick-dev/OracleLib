/*
 * Delegate.h
 *
 *  Created on: 11 Jun 2013
 *      Author: GBY18020
 */

#ifndef DELEGATE_H_
#define DELEGATE_H_

// Two parameter delegate

template<typename Ret, typename Param0, typename Param1>
class Callback {
	public:
		virtual Ret invoke(Param0 param0, Param1 param1) = 0;
};

template<typename Ret, typename Param0, typename Param1>
class StaticFunctionCallback: public Callback<Ret, Param0, Param1> {
	private:
		Ret (*func_)(Param0, Param1);

	public:
		StaticFunctionCallback(Ret (*func)(Param0, Param1)) :
				func_(func)
		{
		}

		virtual Ret invoke(Param0 param0, Param1 param1)
		{
			return (*func_)(param0, param1);
		}
};

template<typename Ret, typename Param0, typename Param1, typename T, typename Method>
class MethodCallback: public Callback<Ret, Param0, Param1> {
	private:
		void *object_;
		Method method_;

	public:
		MethodCallback(void *object, Method method) :
				object_(object), method_(method)
		{
		}

		virtual Ret invoke(Param0 param0, Param1 param1)
		{
			T *obj = static_cast<T *>(object_);
			return (obj->*method_)(param0, param1);
		}
};

template<typename Ret, typename Param0, typename Param1>
class Delegate {
	private:
		Callback<Ret, Param0, Param1> *callback_;

	public:
		Delegate(Ret (*func)(Param0, Param1)) :
				callback_(new StaticFunctionCallback<Ret, Param0, Param1>(func))
		{
		}

		template<typename T, typename Method>
		Delegate(T *object, Method method) :
				callback_(new MethodCallback<Ret, Param0, Param1, T, Method>(object, method))
		{
		}

		~Delegate(void)
		{
			delete callback_;
		}

		Ret operator()(Param0 param0, Param1 param1)
		{
			return callback_->invoke(param0, param1);
		}
};

#endif /* DELEGATE_H_ */
